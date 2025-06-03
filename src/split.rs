use lazy_static::lazy_static;
use regex::Regex;
use std::{collections::{HashMap, HashSet}, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::sql::{get_columns, parse_insert_parts};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Clone)]
enum SqlStatementParts {
    Generic(String),
    TableUnlock(String),
    TableDataDumpComment(String),
    InlineTable(String),
    CreateTable(String),
    Insert { table: String, columns_part: String, values_part: String },
}

impl SqlStatementParts {
    fn new(st: &str) -> Result<Self, anyhow::Error> {
        if st.starts_with("UNLOCK TABLES;") {
            return Ok(SqlStatementParts::TableUnlock(st.to_string()));
        }
        if st.starts_with("-- Dumping data for table") {
            return Ok(SqlStatementParts::TableDataDumpComment(st.to_string()));
        }
        if st.starts_with("UNLOCK TABLES;") {
            return Ok(SqlStatementParts::TableUnlock(st.to_string()));
        }
        if st.starts_with("--- INLINE") {
            return Ok(SqlStatementParts::InlineTable(st.to_string()));
        }
        if st.starts_with("CREATE TABLE") {
            return Ok(SqlStatementParts::CreateTable(st.to_string()));
        }
        if st.starts_with("INSERT") {
            let (table, columns_part, values_part) = parse_insert_parts(st)?;

            return Ok(SqlStatementParts::Insert {
                table,
                columns_part,
                values_part,
            });
        }

        Ok(SqlStatementParts::Generic(st.to_string()))
    }
}

#[derive(Clone)]
pub struct SqlStatement {
    table: Option<String>,
    statement: String,
    parts: SqlStatementParts,
}

impl SqlStatement {
    fn new(statement: &str) -> Result<Self, anyhow::Error> {
        Ok(SqlStatement {
            table: None,
            statement: statement.to_owned(),
            parts: SqlStatementParts::new(statement)?,
        })
    }

    fn set_table(&mut self, table: &Option<String>) {
        self.table = table.to_owned();
    }

    fn get_table(&self) -> &Option<String> {
        &self.table
    }

    fn as_bytes(&self) -> Vec<u8> {
        let bytes: Vec<u8> = match &self.parts {
            SqlStatementParts::Generic(s)
            | SqlStatementParts::CreateTable(s)
            | SqlStatementParts::TableUnlock(s)
            | SqlStatementParts::TableDataDumpComment(s)
            | SqlStatementParts::InlineTable(s)
                => s.to_owned().into_bytes(),
            SqlStatementParts::Insert{ table, columns_part, values_part } => {
                Vec::from(format!("INSERT INTO `{table}` ({columns_part}) VALUES ({values_part});\n").as_bytes())
            },
        };
        bytes
    }

    fn get_column_positions(&self) -> HashMap<String, usize> {
        get_columns(&self.statement).iter().enumerate().map(|(idx, c)| (c.to_owned(), idx)).collect()
    }

    fn is_table_unlock(&self) -> bool {
        matches!(&self.parts, SqlStatementParts::TableUnlock(_))
    }

    fn is_insert(&self) -> bool {
        matches!(&self.parts, SqlStatementParts::Insert{ table: _, columns_part: _, values_part: _ })
    }

    fn get_inline_file(&self) -> Option<PathBuf> {
        match &self.parts {
            SqlStatementParts::InlineTable(line) => Some(PathBuf::from(line.replace("--- INLINE ", "").replace("\n", ""))),
            _ => None,
        }
    }

    fn get_data_types(&self) -> Option<HashMap<String, sqlparser::ast::DataType>> {
        match &self.parts {
            SqlStatementParts::CreateTable(st) => {
                let mut data_types = HashMap::new();
                let dialect = MySqlDialect {};
                let ast = SqlParser::parse_sql(&dialect, st).unwrap();
                for st in ast.into_iter().filter(|x| matches!(x, sqlparser::ast::Statement::CreateTable(_))) {
                    if let sqlparser::ast::Statement::CreateTable(ct) = st {
                        for column in ct.columns.into_iter() {
                            data_types.insert(ct.name.0[0].as_ident().unwrap().value.to_string() + "." + column.name.value.as_str(), column.data_type);
                        }
                    }
                }
                Some(data_types)
            },
            _ => None,
        }
    }

    fn extract_table(&self) -> Option<&str>{
        match &self.parts {
            SqlStatementParts::TableDataDumpComment(comment) => Some(TABLE_DUMP_RE.captures(comment).unwrap().get(1).unwrap().as_str()),
            _ => None,
        }
    }
}

type DataTypes = HashMap<String, sqlparser::ast::DataType>;
type IteratorItem = Result<SqlStatement, anyhow::Error>;

pub struct PlainStatements {
    buf: io::BufReader<fs::File>,
    tracking: bool,
    data_types: DataTypes,
    cur_table: Option<String>,
    unlock_next: bool,
    column_positions: HashMap<String, HashMap<String, usize>>,
}

impl PlainStatements {
    pub fn from_file(sqldump_filepath: &Path, tracking: &bool, curr_table: &Option<String>) -> Result<Self, anyhow::Error> {
        let file = fs::File::open(sqldump_filepath)?;
        Ok(PlainStatements {
            buf: io::BufReader::new(file),
            tracking: tracking.to_owned(),
            data_types: HashMap::new(),
            cur_table: curr_table.to_owned(),
            unlock_next: false,
            column_positions: HashMap::new(),
        })
    }

    pub fn read_statement(&mut self) -> Option<IteratorItem> {
        let mut buf: String = String::new();

        while {
            let read_bytes = self.buf.read_line(&mut buf).ok()?;
            read_bytes > 0 && !PlainStatements::is_full_line(&buf)
        } {}

        match buf.is_empty() {
            true => None,
            false => Some(SqlStatement::new(&buf)),
        }
    }

    pub fn is_full_line(line: &str) -> bool {
        if line.ends_with(";\n") {
            return true;
        }

        if line.starts_with("\n") {
            return true;
        }

        if line.starts_with("--") {
            return true;
        }

        false
    }

    fn capture_table(&mut self, cur_statement: &IteratorItem) {
        if let Ok(st) = cur_statement {
            if self.unlock_next {
                self.unlock_next = false;
                self.cur_table = None;
            }
            if let Some(table) = st.extract_table() {
                println!("reading table {}", &table);
                self.cur_table = Some(table.to_string());
            }
        }
    }

    fn capture_positions(&mut self, cur_statement: &IteratorItem) {
        if let Ok(st) = cur_statement {
            if let Some(ref table) = self.cur_table {
                if !self.column_positions.contains_key(table) && st.is_insert() {
                    self.column_positions.insert(table.clone(), st.get_column_positions());
                };
            }
        }
    }

    fn capture(&mut self, cur_statement: &mut IteratorItem) {
        self.capture_table(cur_statement);
        self.capture_positions(cur_statement);
        if let Ok(st) = cur_statement {
            if st.is_table_unlock() {
                self.unlock_next = true;
            }
            st.set_table(&self.cur_table);
        }
    }
}

impl Iterator for PlainStatements {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut statement = self.read_statement()?;
        if self.tracking {
            self.capture(&mut statement);
        }
        if let Ok(st) = &statement {
            if let Some(data_types) = st.get_data_types() {
                for (key, data_type) in data_types {
                    self.data_types.insert(key, data_type);
                }
            }
        }
        Some(statement)
    }
}

pub fn get_writer(filepath: &Path) -> Result<BufWriter<File>, anyhow::Error> {
    fs::File::create(filepath)?;
    let file = fs::OpenOptions::new()
        .append(true)
        .open(filepath)?;
    Ok(BufWriter::new(file))
}

pub fn explode_to_files(working_file_path: &Path, working_dir_path: &Path, sqldump_filepath: &Path, allowed_tables: &Option<HashSet<String>>) -> Result<HashMap<String, PathBuf>, anyhow::Error> {
    let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut table_files: HashMap<String, PathBuf> = HashMap::new();
    let mut working_file_writer = get_writer(working_file_path)?;

    let statements = PlainStatements::from_file(sqldump_filepath, &true, &None)?;

    for st in statements {
        let statement = st?;
        match statement.get_table() {
            None => working_file_writer.write_all(&statement.as_bytes())?,
            Some(table) => {
                if let Some(allowed) = allowed_tables {
                    if !allowed.contains(table) {
                        continue;
                    }
                }
                let writer = match writers.get_mut(table) {
                    None => {
                        let table_file = std::path::absolute(working_dir_path.join(table).with_extension("sql"))?;
                        table_files.insert(table.to_owned(), table_file.to_owned());
                        working_file_writer.write_all(format!("--- INLINE {}\n", table_file.display()).as_bytes())?;
                        writers.insert(table.to_owned(), get_writer(&table_file)?);
                        writers.get_mut(table).unwrap()
                    },
                    Some(w) => w,
                };

                writer.write_all(&statement.as_bytes())?
            }
        }
    };

    working_file_writer.flush()?;
    for w in writers.values_mut() {
        w.flush()?
    }

    Ok(table_files)
}

pub fn gather(working_file_path: &Path, output_path: &Path) -> Result<(), anyhow::Error> {
    let input = PlainStatements::from_file(working_file_path, &false, &None)?;
    let output = File::create(output_path)?;
    let mut writer = BufWriter::new(output);

    for st in input {
        let valid_line = st?;
        let file = valid_line.get_inline_file();
        let cur_bytes = &valid_line.as_bytes();
        match file {
            Some(ref inline_file) => {
                let inline_input = PlainStatements::from_file(inline_file, &false,  &None)?;
                for inline_line in inline_input {
                    writer.write_all(&inline_line?.as_bytes())?;
                }
            },
            None => {
                writer.write_all(cur_bytes)?;
            }
        }
    }
    writer.flush()?;
    Ok(())
}

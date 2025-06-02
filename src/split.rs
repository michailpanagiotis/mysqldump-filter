use lazy_static::lazy_static;
use regex::Regex;
use std::{collections::{HashMap, HashSet}, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::sql::{get_columns, parse_insert_parts};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

pub fn extract_table(sql_comment: &str) -> String {
    TABLE_DUMP_RE.captures(sql_comment).unwrap().get(1).unwrap().as_str().to_string()
}

#[derive(Clone)]
enum SqlStatementParts {
    Generic(String),
    TableUnlock(String),
    TableDataDumpComment(String),
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
            | SqlStatementParts::TableUnlock(s)
            | SqlStatementParts::TableDataDumpComment(s)
                => s.to_owned().into_bytes(),
            SqlStatementParts::Insert{ table, columns_part, values_part } => {
                Vec::from(format!("INSERT INTO `{table}` ({columns_part}) VALUES ({values_part});\n").as_bytes())
            },
        };
        bytes
    }

    fn is_table_data_dump(&self) -> bool {
        match &self.parts {
            SqlStatementParts::TableDataDumpComment(_) => true,
            _ => false,
        }
    }

    fn is_table_unlock(&self) -> bool {
        match &self.parts {
            SqlStatementParts::TableUnlock(_) => true,
            _ => false,
        }
    }

    fn is_insert(&self) -> bool {
        match &self.parts {
            SqlStatementParts::Insert{ table: _, columns_part: _, values_part: _ } => true,
            _ => false,
        }
    }

    fn get_column_positions(&self) -> HashMap<String, usize> {
        get_columns(&self.statement).iter().enumerate().map(|(idx, c)| (c.to_owned(), idx)).collect()
    }
}

pub struct PlainStatements {
    buf: io::BufReader<fs::File>,
}

impl PlainStatements {
    pub fn from_file(sqldump_filepath: &Path) -> Result<Self, anyhow::Error> {
        let file = fs::File::open(sqldump_filepath)?;
        Ok(PlainStatements {
            buf: io::BufReader::new(file),
        })
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
}

impl Iterator for PlainStatements {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        let mut buf: String = String::new();

        while {
            let read_bytes = self.buf.read_line(&mut buf).ok()?;
            read_bytes > 0 && !PlainStatements::is_full_line(&buf)
        } {}

        match buf.is_empty() {
            true => None,
            false => {
                Some(buf)
            }
        }
    }
}

pub struct SqlStatementsWithTable {
    buf: PlainStatements,
    cur_table: Option<String>,
    unlock_next: bool,
    allowed_tables: Option<HashSet<String>>,
    column_positions: HashMap<String, HashMap<String, usize>>,
}

impl SqlStatementsWithTable {
    pub fn from_file(sqldump_filepath: &Path, allowed_tables: &Option<HashSet<String>>, curr_table: &Option<String>) -> Self {
        let buf = PlainStatements::from_file(sqldump_filepath).expect("Cannot open file");
        SqlStatementsWithTable {
            buf,
            cur_table: curr_table.clone(),
            unlock_next: false,
            allowed_tables: allowed_tables.clone(),
            column_positions: HashMap::new(),
        }
    }

    fn is_table_allowed(&self, table: &Option<String>) -> bool {
        self.allowed_tables.as_ref().is_none_or(|allowed| table.as_ref().is_none_or(|t| allowed.contains(t)))
    }

    fn capture_table(&mut self, cur_statement: &SqlStatement) {
        if self.unlock_next {
            self.cur_table = None;
            self.unlock_next = false;
        }
        if cur_statement.statement.starts_with("-- Dumping data for table") {
            let table = extract_table(&cur_statement.statement);
            println!("reading table {}", &table);
            self.cur_table = Some(table);
        }
    }

    fn capture_positions(&mut self, cur_statement: &SqlStatement) {
        if !cur_statement.is_insert() { return; };
        let Some(ref table) = self.cur_table else { return; };
        if self.column_positions.contains_key(table) { return; };
        self.column_positions.insert(
            table.clone(),
            cur_statement.get_column_positions(),
        );
    }

    fn capture(&mut self, cur_statement: &str) -> Result<SqlStatement, anyhow::Error> {
        let mut cur = SqlStatement::new(cur_statement)?;
        self.capture_table(&cur);
        self.capture_positions(&cur);
        if cur.is_table_unlock() {
            self.unlock_next = true;
        }
        cur.set_table(&self.cur_table);
        Ok(cur)
    }

    fn next_item(&mut self) -> Option<Result<SqlStatement, anyhow::Error>> {
        self.buf.next().map(|s| self.capture(&s))
    }
}

impl Iterator for SqlStatementsWithTable {
    type Item = Result<SqlStatement, anyhow::Error>;
    fn next(&mut self) -> Option<Result<SqlStatement, anyhow::Error>> {
        let mut next: Result<SqlStatement, anyhow::Error>;
        while {
            next = self.next_item()?;
            !next.as_ref().is_ok_and(|n| self.is_table_allowed(n.get_table()))
        } {}

        Some(next)
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

    let statements = SqlStatementsWithTable::from_file(sqldump_filepath, allowed_tables, &None);

    for st in statements {
        let statement = st?;
        match statement.get_table() {
            None => working_file_writer.write_all(&statement.as_bytes())?,
            Some(table) => {
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
    let input = PlainStatements::from_file(working_file_path)?;
    let output = File::create(output_path)?;
    let mut writer = BufWriter::new(output);

    for line in input {
        if line.starts_with("--- INLINE ") {
            let inline_file = PathBuf::from(line.replace("--- INLINE ", "").replace("\n", ""));
            let inline_input = PlainStatements::from_file(&inline_file)?;
            for inline_line in inline_input {
                writer.write_all(inline_line.as_bytes())?;
            }
        } else {
            writer.write_all(line.as_bytes())?;
        }
    }
    writer.flush()?;
    Ok(())
}

use lazy_static::lazy_static;
use regex::Regex;
use std::cell::RefCell;
use std::{collections::{HashMap, HashSet}, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::sql::{get_columns, parse_insert_parts};

type Files = HashMap<String, PathBuf>;
type DataTypes = HashMap<String, HashMap<String, sqlparser::ast::DataType>>;
type ColumnPositions = HashMap<String, HashMap<String, usize>>;
type SqlStatementResult = Result<SqlStatement, anyhow::Error>;
type IteratorItem = (SqlStatementResult, Option<Rc<RefCell<Tracker>>>);

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Clone)]
struct InsertStatement {
    table: String,
    columns_part: String,
    values_part: String,
    values: Vec<String>,
}

impl InsertStatement {
    fn new(statement: &str) -> Result<Self, anyhow::Error> {
        let (table, columns_part, values_part) = parse_insert_parts(statement)?;
        let values = values_part.split(',').map(|x| x.to_string()).collect();
        Ok(Self { table, columns_part, values_part, values })
    }

    fn as_bytes(&self) -> Vec<u8> {
        Vec::from(format!("INSERT INTO `{}` ({}) VALUES ({});\n", self.table, self.columns_part, self.values.join(",")).as_bytes())
    }
}

#[derive(Clone)]
enum SqlStatementParts {
    Generic(String),
    TableUnlock(String),
    TableDataDumpComment(String),
    InlineTable(String),
    CreateTable(String),
    Insert(InsertStatement),
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
            return Ok(SqlStatementParts::Insert(InsertStatement::new(st)?));
        }

        Ok(SqlStatementParts::Generic(st.to_string()))
    }
}

#[derive(Clone)]
pub struct SqlStatement {
    statement: String,
    parts: SqlStatementParts,
}

impl SqlStatement {
    fn new(statement: &str) -> Result<Self, anyhow::Error> {
        Ok(SqlStatement {
            statement: statement.to_owned(),
            parts: SqlStatementParts::new(statement)?,
        })
    }

    fn as_bytes(&self) -> Vec<u8> {
        let bytes: Vec<u8> = match &self.parts {
            SqlStatementParts::Generic(s)
            | SqlStatementParts::CreateTable(s)
            | SqlStatementParts::TableUnlock(s)
            | SqlStatementParts::TableDataDumpComment(s)
            | SqlStatementParts::InlineTable(s)
                => s.to_owned().into_bytes(),
            SqlStatementParts::Insert(s) => s.as_bytes(),
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
        matches!(&self.parts, SqlStatementParts::Insert(_))
    }

    fn get_data_types(&self) -> Option<DataTypes> {
        match &self.parts {
            SqlStatementParts::CreateTable(st) => {
                let dialect = MySqlDialect {};
                let ast = SqlParser::parse_sql(&dialect, st).unwrap();
                for st in ast.into_iter().filter(|x| matches!(x, sqlparser::ast::Statement::CreateTable(_))) {
                    if let sqlparser::ast::Statement::CreateTable(ct) = st {
                        let table = ct.name.0[0].as_ident().unwrap().value.to_string();
                        let mut data_types: DataTypes = HashMap::from([(table.to_owned(), HashMap::new())]);
                        for column in ct.columns.into_iter() {
                            data_types.get_mut(&table)?.insert(column.name.value.to_string(), column.data_type);
                        }
                        return Some(data_types);
                    }
                }
                None
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

struct PlainStatements {
    buf: io::BufReader<fs::File>,
}

impl PlainStatements {
    fn from_file(sqldump_filepath: &Path) -> Result<Self, anyhow::Error> {
        let file = fs::File::open(sqldump_filepath)?;
        Ok(PlainStatements {
            buf: io::BufReader::new(file),
        })
    }

    fn is_full_line(line: &str) -> bool {
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
            false => Some(buf),
        }
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct Tracker {
    files: Files,
    data_types: DataTypes,
    column_positions: ColumnPositions,
    current_table: Option<String>,
}

impl Tracker {
    fn new() -> Self {
        Tracker {
            files: HashMap::new(),
            data_types: HashMap::new(),
            column_positions: HashMap::new(),
            current_table: None,
        }
    }

    fn capture_table(&mut self, table: Option<String>) {
        if let Some(t) = &table {
            println!("reading table {}", &t);
        }
        self.current_table = table;
    }

    fn capture_positions(&mut self, statement: &SqlStatement) {
        if let Some(ref table) = self.current_table {
            if !self.column_positions.contains_key(table) && statement.is_insert() {
                self.column_positions.insert(table.to_string(), statement.get_column_positions());
            };
        }
    }

    fn capture_data_types(&mut self, statement: &SqlStatement) {
        if let Some(data_types) = statement.get_data_types() {
            for (key, data_type) in data_types {
                self.data_types.insert(key, data_type);
            }
        }
    }

    fn capture(&mut self, statement: &SqlStatement, unlock_table: bool) {
        self.capture_positions(statement);
        self.capture_data_types(statement);

        if unlock_table {
            self.capture_table(None);
        } else if let Some(table) = statement.extract_table() {
            self.capture_table(Some(table.to_string()));
        }
    }
}

struct TrackedStatements {
    iter: PlainStatements,
    tracking: bool,
    unlock_next: bool,
    tracker: Rc<RefCell<Tracker>>,
}

impl TrackedStatements {
    fn from_file(sqldump_filepath: &Path, tracker: &Rc<RefCell<Tracker>>, tracking: &bool) -> Result<Self, anyhow::Error> {
        Ok(TrackedStatements {
            iter: PlainStatements::from_file(sqldump_filepath)?,
            tracking: tracking.to_owned(),
            unlock_next: false,
            tracker: Rc::clone(tracker),
        })
    }

    fn read_statement(&mut self) -> Option<SqlStatementResult> {
        let next = self.iter.next()?;
        Some(SqlStatement::new(&next))
    }

    fn capture(&mut self, cur_statement: &mut SqlStatementResult) {
        if let Ok(st) = cur_statement {
            self.tracker.borrow_mut().capture(st, self.unlock_next);

            if self.unlock_next {
                self.unlock_next = false;
            }

            if st.is_table_unlock() {
                self.unlock_next = true;
            }
        }
    }
}

impl Iterator for TrackedStatements {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut statement = self.read_statement()?;
        if self.tracking {
            self.capture(&mut statement);
        }
        Some((statement, Some(Rc::clone(&self.tracker))))
    }
}

fn get_writer(filepath: &Path) -> Result<BufWriter<File>, anyhow::Error> {
    fs::File::create(filepath)?;
    let file = fs::OpenOptions::new()
        .append(true)
        .open(filepath)?;
    Ok(BufWriter::new(file))
}

pub fn explode_to_files(working_file_path: &Path, working_dir_path: &Path, sqldump_filepath: &Path, allowed_tables: &Option<HashSet<String>>) -> Result<Tracker, anyhow::Error> {
    let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut table_files: HashMap<String, PathBuf> = HashMap::new();
    let mut working_file_writer = get_writer(working_file_path)?;
    let tracker = Rc::new(RefCell::new(Tracker::new()));

    let statements = TrackedStatements::from_file(sqldump_filepath, &tracker, &true)?;

    for (st, tr) in statements {
        let statement = st?;
        let current_table = tr.unwrap().borrow().current_table.clone();
        match &current_table {
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
                        working_file_writer.write_all(format!("--- INLINE {} {}\n", table_file.display(), table).as_bytes())?;
                        writers.insert(table.to_owned(), get_writer(&table_file)?);
                        writers.get_mut(table).unwrap()
                    },
                    Some(w) => w,
                };

                writer.write_all(&statement.as_bytes())?
            }
        }
    };

    for (table, file) in &table_files {
        tracker.borrow_mut().files.insert(table.clone(), file.clone());
    }

    working_file_writer.flush()?;
    for w in writers.values_mut() {
        w.flush()?
    }

    dbg!(&tracker);

    Ok((*tracker.borrow()).clone())
}

pub fn read_table_file(file: &Path, table: &str, tracker: &Tracker) -> Result<impl Iterator<Item=IteratorItem>, anyhow::Error> {
    let tracker = Rc::new(RefCell::new(tracker.clone()));
    tracker.borrow_mut().capture_table(Some(table.to_string()));
    let statements = TrackedStatements::from_file(file, &tracker, &true)?;
    Ok(statements)
}

pub fn gather(working_file_path: &Path, output_path: &Path, tracker: &Tracker) -> Result<(), anyhow::Error> {
    let input = PlainStatements::from_file(working_file_path)?;
    let output = File::create(output_path)?;
    let mut writer = BufWriter::new(output);

    for statement in input {
        if statement.starts_with("--- INLINE ") {
            let st = statement.replace("--- INLINE ", "").to_string();
            let mut split = st.split(" ");
            let filename = split.next().ok_or(anyhow::anyhow!("cannot parse filename"))?;
            let table = split.next().ok_or(anyhow::anyhow!("cannot parse table"))?;
            let file = PathBuf::from(filename);
            for inline_line in read_table_file(&file, table, tracker)? {
                writer.write_all(&inline_line.0?.as_bytes())?;
            }
        } else {
            writer.write_all(statement.as_bytes())?;
        }
    }
    writer.flush()?;
    Ok(())
}

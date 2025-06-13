use chrono::NaiveDateTime;
use lazy_static::lazy_static;
use regex::Regex;
use std::cell::RefCell;
use std::collections::HashSet;
use std::{collections::HashMap, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::sql::{get_columns, parse_insert_parts, parse_insert_values};

type Files = HashMap<String, PathBuf>;
type TableDataTypes = HashMap<String, sqlparser::ast::DataType>;
type DataTypes = HashMap<String, TableDataTypes>;
type TableColumnPositions = HashMap<String, usize>;
type ColumnPositions = HashMap<String, TableColumnPositions>;
type SqlStatementResult = Result<SqlStatement, anyhow::Error>;
type IteratorItem = (SqlStatementResult, Option<Rc<RefCell<Tracker>>>);
type OptionalTracker<'a> = Option<&'a Rc<RefCell<Tracker>>>;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

pub enum Value<'a> {
    Int {
        string: &'a str,
        parsed: i64
    },
    Date {
        string: &'a str,
        parsed: i64
    },
    String {
        string: &'a str,
        parsed: String,
    },
    Null {
        string: &'a str,
    }
}

impl<'a> Value<'a> {
    pub fn as_string(&'a self) -> &'a str {
        match self {
            Value::Int{ string, .. } => string,
            Value::Date{ string, .. } => string,
            Value::String{ string, .. } => string,
            Value::Null{ string, .. } => string,
        }
    }

    fn parse_int(s: &'a str) -> i64 {
        s.parse().unwrap_or_else(|_| panic!("cannot parse int {s}"))
    }

    fn parse_string(s: &'a str) -> String {
        s.replace("'", "")
    }

    fn parse_date(s: &'a str) -> i64 {
        let date = Value::parse_string(s);
        let to_parse = if date.len() == 10 { date.to_owned() + " 00:00:00" } else { date.to_owned() };
        NaiveDateTime::parse_from_str(&to_parse, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| panic!("cannot parse timestamp {s}"))
            .and_utc()
            .timestamp()
    }

    fn parse(value: &'a str, data_type: &'a sqlparser::ast::DataType) -> Self {
        if value == "NULL" {
            return Value::Null { string: value };
        }
        match data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                Value::Int{ string: value, parsed: Value::parse_int(value) }
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                Value::Date{ string: value, parsed: Value::parse_date(value) }
            },
            _ => Value::String{ string: value, parsed: Value::parse_string(value) }
        }
    }
}

#[derive(Clone)]
#[derive(Debug)]
struct InsertStatement {
    statement: String,
    table: String,
    columns_part: String,
    values_part: String,
    values: Vec<String>,
}

impl InsertStatement {
    fn new(statement: &str) -> Result<Self, anyhow::Error> {
        let (table, columns_part, values_part) = parse_insert_parts(statement)?;
        let values = values_part.split(',').map(|x| x.to_string()).collect();
        Ok(Self { statement: statement.to_string(), table, columns_part, values_part, values })
    }

    fn get_column_positions(&self) -> HashMap<String, usize> {
        get_columns(&self.statement).iter().enumerate().map(|(idx, c)| (c.to_owned(), idx)).collect()
    }

    fn as_string(&self) -> String {
        if self.values.is_empty() {
            format!("INSERT INTO `{}` ({}) VALUES ({});\n", self.table, self.columns_part, self.values_part)
        } else {
            format!("INSERT INTO `{}` ({}) VALUES ({});\n", self.table, self.columns_part, self.values.join(","))
        }
    }

    pub fn get_values(&self) -> Vec<&str> {
        parse_insert_values(&self.values_part)
    }
}

#[derive(Clone)]
#[derive(Debug)]
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
#[derive(Debug)]
pub struct SqlStatement {
    parts: SqlStatementParts,
    table: Option<String>,
}

impl SqlStatement {
    fn new(statement: &str, table: &Option<String>) -> Result<Self, anyhow::Error> {
        Ok(SqlStatement {
            parts: SqlStatementParts::new(statement)?,
            table: table.clone(),
        })
    }

    pub fn as_string(&self) -> String {
        match &self.parts {
            SqlStatementParts::Generic(s)
            | SqlStatementParts::CreateTable(s)
            | SqlStatementParts::TableUnlock(s)
            | SqlStatementParts::TableDataDumpComment(s)
            | SqlStatementParts::InlineTable(s)
                => s.to_owned(),
            SqlStatementParts::Insert(s) => s.as_string(),
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        Vec::from(self.as_string().as_bytes())
    }

    pub fn get_table(&self) -> &Option<String> {
        &self.table
    }

    fn get_column_positions(&self) -> Option<HashMap<String, usize>> {
        match &self.parts {
            SqlStatementParts::Insert(insert_statement) => Some(insert_statement.get_column_positions()),
            _ => None,
        }
    }

    pub fn get_values(&self) -> Option<Vec<&str>> {
        match &self.parts {
            SqlStatementParts::Insert(insert_statement) => Some(insert_statement.get_values()),
            _ => None,
        }
    }

    fn parse_inline_file(&self) -> Result<Option<(String, PathBuf)>, anyhow::Error> {
        match &self.parts {
            SqlStatementParts::InlineTable(line) => {
                let st = line.replace("--- INLINE ", "").replace("\n", "").to_string();
                let mut split = st.split(" ");
                let filename = split.next().ok_or(anyhow::anyhow!("cannot parse filename"))?;
                let table = split.next().ok_or(anyhow::anyhow!("cannot parse table"))?;
                Ok(Some((table.to_string(), PathBuf::from(filename))))
            }
            _ => Ok(None),
        }
    }

    pub fn is_insert(&self) -> bool {
        matches!(&self.parts, SqlStatementParts::Insert(_))
    }

    fn is_table_unlock(&self) -> bool {
        matches!(&self.parts, SqlStatementParts::TableUnlock(_))
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

    fn extract_table(&mut self) -> Option<&str>{
        match &self.parts {
            SqlStatementParts::TableDataDumpComment(comment) => {
                let table = TABLE_DUMP_RE.captures(comment).unwrap().get(1).unwrap().as_str();
                self.table = Some(table.to_string());
                Some(table)
            },
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
}

impl Tracker {
    pub fn new() -> Self {
        Tracker {
            files: HashMap::new(),
            data_types: HashMap::new(),
            column_positions: HashMap::new(),
        }
    }

    pub fn from_working_file_path(filepath: &Path) -> Result<Self, anyhow::Error> {
        let tracker = Rc::new(RefCell::new(Tracker::new()));
        let statements = TrackedStatements::from_file(filepath, &Some(&tracker))?;
        for (_, _) in statements {}
        Ok((*tracker.borrow()).clone())
    }

    fn capture_positions(&mut self, statement: &SqlStatement, current_table: &Option<String>) {
        if let Some(table) = current_table {
            if !self.column_positions.contains_key(table) {
                if let Some(pos) = statement.get_column_positions() {
                    self.column_positions.insert(table.to_string(), pos);
                }
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

    fn capture_inline_files(&mut self, statement: &SqlStatement) -> Result<(), anyhow::Error> {
        if let Some((table, file)) = statement.parse_inline_file()? {
            self.files.insert(table, file);
        }
        Ok(())
    }

    fn capture(&mut self, statement: &SqlStatement, current_table: &Option<String>) -> Result<(), anyhow::Error> {
        self.capture_positions(statement, current_table);
        self.capture_data_types(statement);
        self.capture_inline_files(statement)?;
        Ok(())
    }

    pub fn get_table_file(&self, table: &str) -> &PathBuf {
        &self.files[table]
    }

    pub fn get_table_data_types(&self, table: &str) -> &TableDataTypes {
        &self.data_types[table]
    }

    pub fn get_table_column_positions(&self, table: &str) -> &TableColumnPositions {
        &self.column_positions[table]
    }

    pub fn get_values<'a>(&'a self, insert_statement: &'a SqlStatement) -> Option<HashMap<String, Value<'a>>> {
        let Some(table) = insert_statement.get_table() else {
            return None;
        };

        let data_types = self.get_table_data_types(table);
        let positions = self.get_table_column_positions(table);
        let values = insert_statement.get_values().unwrap();
        Some(positions.iter().map(|(column_name, position)| (column_name.to_owned(), Value::parse(values[*position], &data_types[column_name]))).collect())
    }
}

struct TrackedStatements {
    iter: PlainStatements,
    current_table: Option<String>,
    unlock_next: bool,
    tracker: Option<Rc<RefCell<Tracker>>>,
}

impl TrackedStatements {
    fn from_file(sqldump_filepath: &Path, tracker: &OptionalTracker<'_>) -> Result<Self, anyhow::Error> {
        Ok(TrackedStatements {
            iter: PlainStatements::from_file(sqldump_filepath)?,
            current_table: None,
            unlock_next: false,
            tracker: tracker.map(Rc::clone),
        })
    }

    fn capture_table(&mut self, table: Option<String>) {
        if let Some(t) = &table {
            println!("reading table {}", &t);
        }
        self.current_table = table;
    }

    fn read_statement(&mut self) -> Option<SqlStatementResult> {
        let next = self.iter.next()?;
        Some(SqlStatement::new(&next, &self.current_table))
    }
}

impl Iterator for TrackedStatements {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut statement = self.read_statement()?;

        if let Ok(st) = &mut statement {
            if self.unlock_next {
                self.capture_table(None);
                self.unlock_next = false;
            } else if let Some(table) = st.extract_table() {
                self.capture_table(Some(table.to_string()));
            }

            if st.is_table_unlock() {
                self.unlock_next = true;
            }

            if let Some(tracker) = &self.tracker {
                tracker.borrow_mut().capture(st, &self.current_table);
            }
        }

        Some((statement, self.tracker.as_ref().map(Rc::clone)))
    }
}

fn get_writer(filepath: &Path) -> Result<BufWriter<File>, anyhow::Error> {
    fs::File::create(filepath)?;
    let file = fs::OpenOptions::new()
        .append(true)
        .open(filepath)?;
    Ok(BufWriter::new(file))
}

pub fn explode_to_files<F>(
    working_file_path: &Path,
    working_dir_path: &Path,
    sqldump_filepath: &Path,
    transform: F,
) -> Result<Tracker, anyhow::Error>
  where F: Fn(&SqlStatement, &Tracker) -> Option<SqlStatement>
{
    let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut table_files: HashMap<String, PathBuf> = HashMap::new();
    let mut working_file_writer = get_writer(working_file_path)?;
    let tracker = Rc::new(RefCell::new(Tracker::new()));

    let statements = TrackedStatements::from_file(sqldump_filepath, &Some(&tracker))?;

    for (st, _) in statements {
        let transformed = transform(&st?, &tracker.borrow());
        if let Some(statement) = transformed {
            match statement.get_table() {
                None => working_file_writer.write_all(&statement.as_bytes())?,
                Some(table) => {
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

fn prepare_tracking_hashmap(tracked_columns: &[&str]) -> Result<(HashMap<String, String>, HashMap<String, HashSet<String>>), anyhow::Error> {
    let mut captured: HashMap<String, HashSet<String>> = HashMap::new();
    let mut column_per_key: HashMap<String, String> = HashMap::new();
    for key in tracked_columns {
        captured.insert(key.to_string(), HashSet::new());
        let mut split = key.split('.');
        let (Some(table), Some(column), None) = (split.next(), split.next(), split.next()) else {
            return Err(anyhow::anyhow!("malformed key {}", key));
        };
        column_per_key.insert(key.to_string(), column.to_owned());
    }
    Ok((column_per_key, captured))
}

pub fn process_table_inserts<F>(
    working_file_path: &Path,
    table: &str,
    tracked_columns: &[&str],
    mut transform: F,
) -> Result<HashMap<String, HashSet<String>>, anyhow::Error>
  where F: FnMut(&SqlStatement, &HashMap<String, Value<'_>>) -> Result<Option<SqlStatement>, anyhow::Error>
{
    println!("Processing table {table}");
    let tracker = Tracker::from_working_file_path(working_file_path)?;
    let table_file = tracker.get_table_file(table);
    let output_file = &table_file.with_extension("proc");
    let mut writer = get_writer(output_file)?;

    let statements = TrackedStatements::from_file(table_file, &Some(&Rc::new(RefCell::new(tracker.clone()))))?;

    let (column_per_key, mut captured) = prepare_tracking_hashmap(tracked_columns)?;

    for (st, tr_option) in statements {
        let input_statement = st?;
        let tr = tr_option.ok_or(anyhow::anyhow!("unknown tracker"))?;
        if input_statement.get_table().as_ref().is_none_or(|t| t != table) {
            return Err(anyhow::anyhow!("wrong table"));
        }
        // filter inserts
        if input_statement.is_insert() {
            let borrowed = tr.borrow();
            let Some(value_per_field) = borrowed.get_values(&input_statement) else {
                return Err(anyhow::anyhow!("cannot get values"));
            };
            if let Some(statement) = transform(&input_statement, &value_per_field)? {
                // capture values
                for (key, column) in &column_per_key {
                    let value = &value_per_field[column];
                    if let Some(set) = captured.get_mut(key) {
                        set.insert(value.as_string().to_owned());
                    }
                }

                writer.write_all(&statement.as_bytes())?;
            }
        } else {
            writer.write_all(&input_statement.as_bytes())?;
        }
    };

    //fs::rename(output_file, table_file).expect("cannot rename");

    Ok(captured)
}

pub fn read_table_file(working_file_path: &Path, table: &str) -> Result<impl Iterator<Item=IteratorItem>, anyhow::Error> {
    let tracker = Rc::new(RefCell::new(Tracker::from_working_file_path(working_file_path)?));

    let binding = tracker.borrow();
    let table_file = binding.get_table_file(table);
    let statements = TrackedStatements::from_file(table_file, &Some(&tracker))?;
    Ok(statements)
}

pub fn get_table_files(working_file_path: &Path) -> Result<HashMap<String, PathBuf>, anyhow::Error> {
    let mut table_files: HashMap<String, PathBuf> = HashMap::new();
    let file = File::open(working_file_path)?;
    for res in io::BufReader::new(file).lines() {
        let line = res?;
        if line.starts_with("--- INLINE ") {
            let st = line.replace("--- INLINE ", "").to_string();
            let mut split = st.split(" ");
            let filename = split.next().ok_or(anyhow::anyhow!("cannot parse filename"))?;
            let table = split.next().ok_or(anyhow::anyhow!("cannot parse table"))?;
            table_files.insert(table.to_string(), PathBuf::from(filename));
        }
    }
    Ok(table_files)
}

pub fn gather(working_file_path: &Path, output_path: &Path) -> Result<(), anyhow::Error> {
    let output = File::create(output_path)?;
    let mut writer = BufWriter::new(output);

    let file = File::open(working_file_path)?;

    for res in io::BufReader::new(file).lines() {
        let line = res?;
        if line.starts_with("--- INLINE ") {
            let st = line.replace("--- INLINE ", "").to_string();
            let mut split = st.split(" ");
            let filename = split.next().ok_or(anyhow::anyhow!("cannot parse filename"))?;
            println!("INLINING {filename}");
            let inline_file = File::open(PathBuf::from(filename))?;
            for inline_line in io::BufReader::new(inline_file).lines() {
                writer.write_all(inline_line?.as_bytes())?;
                writer.write_all(b"\n")?;
            }
        } else {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }
    writer.flush()?;
    Ok(())
}

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

use crate::sql_parser::{insert_parts, values};

type Files = HashMap<Option<String>, PathBuf>;
type TableDataTypes = Rc<HashMap<String, sqlparser::ast::DataType>>;
type DataTypes = HashMap<String, TableDataTypes>;
type TableColumnPositions = Rc<HashMap<String, usize>>;
type ColumnPositions = HashMap<String, TableColumnPositions>;
type IteratorItem = SqlStatementResult;
type CapturedValues = HashMap<String, HashSet<String>>;
type TrackerCell = Rc<RefCell<Tracker>>;

type SqlStatementResult = Result<SqlStatement, anyhow::Error>;
type OptionalStatementResult = Result<Option<()>, anyhow::Error>;
type EmptyResult = Result<(), anyhow::Error>;

type Values = HashMap<String, Value>;

// trait alias for transform functions
pub trait TransformFn: FnMut(&mut SqlStatement) -> OptionalStatementResult  {}
impl<T: FnMut(&mut SqlStatement) -> OptionalStatementResult> TransformFn for T {}

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Clone)]
#[derive(Debug)]
pub enum Value {
    Int {
        string: String,
        parsed: i64
    },
    Date {
        string: String,
        parsed: i64
    },
    String {
        string: String,
        parsed: String,
    },
    Null {
        string: String,
    }
}

impl Value {
    pub fn as_string(&self) -> &str {
        match self {
            Value::Int{ string, .. } => string.as_str(),
            Value::Date{ string, .. } => string.as_str(),
            Value::String{ string, .. } => string.as_str(),
            Value::Null{ string, .. } => string.as_str(),
        }
    }

    fn parse_int(s: &str) -> i64 {
        s.parse().unwrap_or_else(|_| panic!("cannot parse int {s}"))
    }

    fn parse_string(s: &str) -> String {
        s.replace("'", "")
    }

    fn parse_date(s: &str) -> i64 {
        let date = Value::parse_string(s);
        let to_parse = if date.len() == 10 { date.to_owned() + " 00:00:00" } else { date.to_owned() };
        if to_parse.starts_with("0000-00-00") {
            return NaiveDateTime::MIN.and_utc().timestamp();
        }
        NaiveDateTime::parse_from_str(&to_parse, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| panic!("cannot parse timestamp {s}"))
            .and_utc()
            .timestamp()
    }

    fn parse(value: &str, data_type: &sqlparser::ast::DataType) -> Self {
        if value == "NULL" {
            return Value::Null { string: value.to_string() };
        }
        match data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                Value::Int{ string: value.to_string(), parsed: Value::parse_int(value) }
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                Value::Date{ string: value.to_string(), parsed: Value::parse_date(value) }
            },
            _ => Value::String{ string: value.to_string(), parsed: Value::parse_string(value) }
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
    data_types: Option<TableDataTypes>,
    positions: Option<TableColumnPositions>,
    value_per_field: Option<HashMap<String, Value>>,
}

impl InsertStatement {
    fn new(statement: &str) -> Result<Self, anyhow::Error> {
        let (table, columns_part, values_part) = insert_parts(statement)?;
        Ok(Self { statement: statement.to_string(), table, columns_part, values_part, values: Vec::new(), value_per_field: None, data_types: None, positions: None })
    }

    fn get_column_positions(&self) -> HashMap<String, usize> {
        let dialect = MySqlDialect {};
        let ast = SqlParser::parse_sql(&dialect, &self.statement).unwrap();

        let st = ast.first().unwrap();
        let sqlparser::ast::Statement::Insert(x) = st else { panic!("stop") };

        x.columns.iter().enumerate().map(|(idx, x)| (x.value.to_owned(), idx)).collect()
    }

    fn as_string(&self) -> &str {
        &self.statement
    }

    fn set_meta(&mut self, column_positions: &TableColumnPositions, data_types: &TableDataTypes) {
        self.positions = Some(Rc::clone(column_positions));
        self.data_types = Some(Rc::clone(data_types));
    }

    pub fn get_values(&mut self) -> Result<&HashMap<String, Value>, anyhow::Error> {
        if self.value_per_field.is_none() {
            let Some(ref positions) = self.positions else {
                return Err(anyhow::anyhow!("statement with no positions"));
            };
            let Some(ref data_types) = self.data_types else {
                return Err(anyhow::anyhow!("statement with no data types"));
            };
            let value_array = self.get_value_array()?;
            let values: Values = positions
                .iter()
                .map(|(column_name, position)| {
                    (
                        column_name.to_owned(),
                        Value::parse(value_array[*position], &data_types[column_name])
                    )
                })
                .collect::<Values>();
            self.value_per_field = Some(values);
        }
        let Some(ref values) = self.value_per_field else {
            return Err(anyhow::anyhow!("cannot get empty values"));
        };
        Ok(values)
    }

    fn get_value_array(&self) -> Result<Vec<&str>, anyhow::Error> {
        match values(&self.values_part) {
            Err(_) => Err(anyhow::anyhow!("cannot parse values")),
            Ok((_, values)) => Ok(values)
        }
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
}

impl SqlStatement {
    fn new(statement: &str, table: &Option<String>) -> Result<Self, anyhow::Error> {
        Ok(SqlStatement {
            parts: SqlStatementParts::new(statement)?,
        })
    }

    pub fn as_string(&self) -> &str {
        match &self.parts {
            SqlStatementParts::Generic(s)
            | SqlStatementParts::CreateTable(s)
            | SqlStatementParts::TableUnlock(s)
            | SqlStatementParts::TableDataDumpComment(s)
            | SqlStatementParts::InlineTable(s)
                => s,
            SqlStatementParts::Insert(s) => s.as_string(),
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        Vec::from(self.as_string().as_bytes())
    }

    pub fn get_table(&self) -> Option<&str> {
        match &self.parts {
            SqlStatementParts::Insert(insert_statement) => Some(&insert_statement.table),
            _ => None,
        }
    }

    fn get_column_positions(&self) -> Option<HashMap<String, usize>> {
        match &self.parts {
            SqlStatementParts::Insert(insert_statement) => Some(insert_statement.get_column_positions()),
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
                        let data_types: DataTypes = HashMap::from([
                            (table.to_owned(), Rc::new(
                                HashMap::from_iter(
                                    ct.columns.iter().map(|column| (column.name.value.to_string(), column.data_type.to_owned())),
                                ),
                            )),
                        ]);
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
                Some(table)
            },
            _ => None,
        }
    }

    pub fn get_values(&mut self) -> Result<&HashMap<String, Value>, anyhow::Error> {
        match self.parts {
            SqlStatementParts::Insert(ref mut insert_statement) => {
                insert_statement.get_values()
            },
            _ => Err(anyhow::anyhow!("can only get values of insert statements")),
        }
    }
}

impl<'a> TryFrom<&'a mut SqlStatement> for &'a mut InsertStatement {
    type Error = anyhow::Error;

    fn try_from(other: &'a mut SqlStatement) -> Result<&'a mut InsertStatement, Self::Error> {
        match other.parts {
            SqlStatementParts::Insert(ref mut insert_statement) => Ok(insert_statement),
            _ => Err(anyhow::anyhow!("cannot convert statement to insert statement")),
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
    working_dir_path: PathBuf,
    working_file_path: PathBuf,
    in_place: bool,
    files: Files,
    data_types: DataTypes,
    column_positions: ColumnPositions,
    captured_values: CapturedValues,
    tracked_column_per_key: HashMap<String, String>,
    inline_files: HashSet<PathBuf>,
    working_file_writer: Option<Rc<RefCell<BufWriter<File>>>>,
    current_writer: Option<Rc<RefCell<BufWriter<File>>>>,
    current_file: Option<PathBuf>,
}

impl Tracker {
    fn new(working_file_path: &Path, tracked_columns: &[&str], in_place: bool) -> Result<Rc<RefCell<Self>>, anyhow::Error> {
        let working_dir_path = working_file_path.parent().ok_or(anyhow::anyhow!("cannot find parent directory"))?;
        let (captured_values, tracked_column_per_key) = Tracker::prepare_tracked_columns(tracked_columns)?;
        Ok(Rc::new(RefCell::new(Tracker {
            working_dir_path: working_dir_path.to_owned(),
            working_file_path: working_file_path.to_owned(),
            in_place,
            files: HashMap::new(),
            data_types: HashMap::new(),
            column_positions: HashMap::new(),
            captured_values,
            tracked_column_per_key,
            inline_files: HashSet::new(),
            working_file_writer: None,
            current_writer: None,
            current_file: None,
        })))
    }

    fn from_working_file_path(working_file_path: &Path, tracked_columns: &[&str]) -> Result<Self, anyhow::Error> {
        let tracker = Tracker::new(working_file_path, tracked_columns, true)?;
        let statements = TrackedStatements::from_file(working_file_path, &tracker)?;
        // consume iterator to populate tracker
        statements.for_each(drop);
        Ok((*tracker.borrow()).clone())
    }

    fn prepare_tracked_columns(tracked_columns: &[&str]) -> Result<(CapturedValues, HashMap<String, String>), anyhow::Error> {
        let mut captured_values: CapturedValues = HashMap::new();
        let mut tracked_column_per_key: HashMap<String, String> = HashMap::new();
        for key in tracked_columns {
            captured_values.insert(key.to_string(), HashSet::new());
            let mut split = key.split('.');
            let (Some(_), Some(column), None) = (split.next(), split.next(), split.next()) else {
                return Err(anyhow::anyhow!("malformed key {}", key));
            };
            tracked_column_per_key.insert(key.to_string(), column.to_owned());
        }
        Ok((captured_values, tracked_column_per_key))
    }

    fn capture_positions(&mut self, statement: &SqlStatement, current_table: &Option<String>) {
        if let Some(table) = current_table {
            if !self.column_positions.contains_key(table) {
                if let Some(pos) = statement.get_column_positions() {
                    self.column_positions.insert(table.to_string(), Rc::new(pos));
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

    fn capture_inline_files(&mut self, statement: &SqlStatement) -> EmptyResult {
        if let Some((table, file)) = statement.parse_inline_file()? {
            self.files.insert(Some(table), file);
        }
        Ok(())
    }

    fn capture(&mut self, statement: &SqlStatement, current_table: &Option<String>) -> EmptyResult {
        self.capture_positions(statement, current_table);
        self.capture_data_types(statement);
        self.capture_inline_files(statement)?;
        Ok(())
    }

    fn get_table_file(&self, table: &str) -> Result<PathBuf, io::Error> {
        std::path::absolute(self.working_dir_path.join(table).with_extension("sql"))
    }

    fn get_table_files(&self) -> &HashMap<Option<String>, PathBuf> {
        &self.files
    }

    fn get_table_data_types(&self, table: &str) -> &TableDataTypes {
        &self.data_types[table]
    }

    fn get_table_column_positions(&self, table: &str) -> &TableColumnPositions {
        &self.column_positions[table]
    }

    fn capture_values(&mut self, value_per_field: &HashMap<String, Value>) {
        if self.is_capturing_columns() {
            for (key, column) in &self.tracked_column_per_key {
                let value = &value_per_field[column];
                if let Some(set) = self.captured_values.get_mut(key) {
                    set.insert(value.as_string().to_owned());
                }
            }
        }
    }

    fn get_captured_values(&self) -> &CapturedValues {
        &self.captured_values
    }

    fn is_capturing_columns(&self) -> bool {
        !self.tracked_column_per_key.is_empty()
    }

    fn try_share_meta(&self, statement: &mut SqlStatement) -> EmptyResult {
        if let Ok(i) = <&mut InsertStatement>::try_from(statement) {
            let positions = self.get_table_column_positions(&i.table);
            let data_types = self.get_table_data_types(&i.table);
            i.set_meta(positions, data_types);
        }
        Ok(())
    }

    fn determine_output_file(&self, table_option: &Option<&str>) -> Result<PathBuf, anyhow::Error> {
        match table_option {
            None => {
                if self.in_place {
                    return Err(anyhow::anyhow!("cannot write to working file in place"));
                }
                Ok(self.working_file_path.to_owned())
            }
            Some(table) => {
                let table_file = std::path::absolute(
                    if self.in_place {
                        self.working_dir_path.join(table).with_extension("proc")
                    } else {
                        self.working_dir_path.join(table).with_extension("sql")
                    }
                )?;
                Ok(table_file)
            }
        }
    }

    fn set_writer(&mut self, table: &Option<String>, filepath: &Option<PathBuf>, writer: &Option<Rc<RefCell<BufWriter<File>>>>) {
        if table.is_none() && self.working_file_writer.is_none() {
            self.working_file_writer = writer.as_ref().map(Rc::clone);
        }
        self.current_writer = writer.as_ref().map(Rc::clone);
        self.current_file = filepath.to_owned();
    }

    fn try_write_inline_file(&mut self, table_option: &Option<&str>, filepath: &Path) -> Result<(), anyhow::Error> {
        if !self.inline_files.contains(filepath) {
            self.inline_files.insert(filepath.to_owned());
            println!("inlining file {}", &filepath.display());
            if let Some(table) = table_option {
                let Some(ref working_file_writer) = self.working_file_writer else {
                    return Err(anyhow::anyhow!("cannot find output file"));
                };
                working_file_writer.borrow_mut().write_all(format!("--- INLINE {} {}\n", filepath.display(), table).as_bytes())?;
            };
        }
        Ok(())
    }

    fn transform_statement<F>(
        &mut self,
        input_statement: &mut SqlStatement,
        mut transform: F,
    ) -> OptionalStatementResult
        where F: TransformFn
    {
        if input_statement.is_insert() {
            let transformed = (transform)(input_statement)?;
            if self.is_capturing_columns() {
                let value_per_field = input_statement.get_values()?;
                self.capture_values(value_per_field);
            }
            return Ok(transformed);
        }
        Ok(Some(()))
    }

    fn flush(&self) -> Result<(), anyhow::Error> {
        if let Some(w) = &self.current_writer {
            w.borrow_mut().flush()?;
        }
        if let Some(w) = &self.working_file_writer {
            w.borrow_mut().flush()?;
        }
        Ok(())
    }
}

struct TrackedStatements {
    iter: PlainStatements,
    current_table: Option<String>,
    unlock_next: bool,
    tracker: Rc<RefCell<Tracker>>,
}

impl TrackedStatements {
    fn from_file(sqldump_filepath: &Path, tracker: &TrackerCell) -> Result<Self, anyhow::Error> {
        Ok(TrackedStatements {
            iter: PlainStatements::from_file(sqldump_filepath)?,
            current_table: None,
            unlock_next: false,
            tracker: Rc::clone(tracker),
        })
    }

    fn capture_table(&mut self, table: Option<String>) -> Result<(), anyhow::Error> {
        if let Some(t) = &table {
            println!("reading table {}", &t);
        }
        self.current_table = table;
        Ok(())
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

            if let Err(e) = self.tracker.borrow_mut().capture(st, &self.current_table) {
                return Some(Err(e));
            }
        }

        Some(statement)
    }
}

struct TransformedStatements<F: TransformFn> {
    iter: TrackedStatements,
    current_table: Option<String>,
    current_writer: Option<Rc<RefCell<BufWriter<File>>>>,
    current_file: Option<PathBuf>,
    written_files: HashSet<PathBuf>,
    transform: F,
}

impl<F: TransformFn> TransformedStatements<F> {
    fn from_file(sqldump_filepath: &Path, tracker: &TrackerCell, transform: F) -> Result<Self, anyhow::Error> {
        Ok(TransformedStatements {
            iter: TrackedStatements::from_file(sqldump_filepath, tracker)?,
            current_table: None,
            current_writer: None,
            current_file: None,
            written_files: HashSet::new(),
            transform,
        })
    }

    fn determine_writer(&mut self, statement: &SqlStatement) -> Result<(), anyhow::Error> {
        let table_option = statement.get_table();
        if self.current_writer.is_none() || table_option != self.current_table.as_deref() {
            self.current_table = table_option.map(|s| s.to_owned());
            let filepath = self.iter.tracker.borrow().determine_output_file(&table_option)?;
            self.current_file = Some(filepath.to_owned());
            if !self.written_files.contains(&filepath) {
                println!("creating file {}", &filepath.display());
                self.written_files.insert(filepath.to_owned());
                fs::File::create(&filepath)?;
            } else {
                println!("appending to file {}", &filepath.display());
            }
            let file = fs::OpenOptions::new().append(true).open(&filepath)?;
            if let Some(writer) = &self.current_writer {
                writer.borrow_mut().flush();
            }
            self.current_writer = Some(Rc::new(RefCell::new(BufWriter::new(file))));
        }
        Ok(())
    }

    fn transform_iteration_item(&mut self, mut item: IteratorItem) -> Option<IteratorItem> {
        match item {
            Err(_) => Some(item),
            Ok(ref mut st) => {
                match self.determine_writer(st) {
                    Err(_) => Some(Err(anyhow::anyhow!("cannot determine writer"))),
                    Ok(_) => {
                        self.iter.tracker.borrow_mut().set_writer(&self.current_table, &self.current_file, &self.current_writer);
                        if self.iter.tracker.borrow().try_share_meta(st).is_err() {
                            return Some(Err(anyhow::anyhow!("cannot share meta")));
                        }
                        match self.iter.tracker.borrow_mut().transform_statement(st, &mut self.transform) {
                            Err(e) => Some(Err(e)),
                            Ok(transformed_option) => {
                                transformed_option.map(|()| { item })
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<F: TransformFn> Iterator for TransformedStatements<F> {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut transformed;

        while {
            let input_statement = self.iter.next()?;
            transformed = self.transform_iteration_item(input_statement);
            transformed.is_none()
        } {}

        transformed
    }
}

fn process_input_file<F: TransformFn>(
    sqldump_filepath: &Path,
    tracker_cell: &TrackerCell,
    transform: F,
    in_place: bool,
) -> EmptyResult {
    let statements = TransformedStatements::from_file(sqldump_filepath, tracker_cell, transform)?;
    for st in statements {
        let statement = st?;
        let filepath_option = tracker_cell.borrow().current_file.to_owned();
        let mut borrowed = tracker_cell.borrow_mut();
        let Some(writer) = &mut borrowed.current_writer else {
            return Err(anyhow::anyhow!("cannot find writer"));
        };
        let Some(filepath) = &filepath_option else {
            return Err(anyhow::anyhow!("cannot find output file"));
        };

        writer.borrow_mut().write_all(&statement.as_bytes())?;

        if !in_place {
            borrowed.try_write_inline_file(&statement.get_table(), filepath)?;
        }
    };
    tracker_cell.borrow_mut().flush()?;
    Ok(())
}

pub fn explode_to_files<F>(
    working_file_path: &Path,
    sqldump_filepath: &Path,
    transform: F,
) -> Result<CapturedValues, anyhow::Error>
  where F: TransformFn
{
    let tracker_cell = Tracker::new(working_file_path, &[], false)?;
    process_input_file(sqldump_filepath, &tracker_cell, transform, false)?;

    Ok(tracker_cell.borrow().get_captured_values().clone())
}

pub fn process_table_inserts<F>(
    working_file_path: &Path,
    table: &str,
    tracked_columns: &[&str],
    transform: F,
) -> Result<CapturedValues, anyhow::Error>
  where F: TransformFn
{
    println!("Processing table {table}");

    let tracker_cell = Rc::new(RefCell::new(Tracker::from_working_file_path(working_file_path, tracked_columns)?));
    let sqldump_filepath = &tracker_cell.borrow().get_table_file(table)?;

    process_input_file(sqldump_filepath, &tracker_cell, transform, true)?;

    Ok(tracker_cell.borrow().get_captured_values().clone())
}

#[allow(dead_code)]
pub fn gather(working_file_path: &Path, output_path: &Path) -> EmptyResult {
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

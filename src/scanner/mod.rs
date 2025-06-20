mod sql_parser;
mod writers;

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

use crate::scanner::sql_parser::{insert_parts, values};
use crate::scanner::writers::Writers;

type TableDataTypes = Rc<HashMap<String, sqlparser::ast::DataType>>;
type DataTypes = HashMap<String, TableDataTypes>;
type TableColumnPositions = Rc<HashMap<String, usize>>;
type ColumnPositions = HashMap<String, TableColumnPositions>;
type IteratorItem = SqlStatementResult;
type CapturedValues = HashMap<String, HashSet<String>>;
type TrackerCell = Rc<RefCell<Tracker>>;

type SqlStatement = (String, Option<String>);
type SqlStatementResult = Result<SqlStatement, anyhow::Error>;
type OptionalStatementResult = Result<Option<()>, anyhow::Error>;
type EmptyResult = Result<(), anyhow::Error>;

type Values = HashMap<String, Value>;

// trait alias for transform functions
pub trait TransformFn: FnMut(&mut InsertStatement) -> OptionalStatementResult  {}
impl<T: FnMut(&mut InsertStatement) -> OptionalStatementResult> TransformFn for T {}

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

fn new_statement(line: String, table: Option<String>) -> SqlStatement {
    (line, table)
}

fn get_data_types(create_statement: &str) -> Result<Option<(String, TableDataTypes)>, anyhow::Error> {
    let dialect = MySqlDialect {};
    let ast = SqlParser::parse_sql(&dialect, create_statement)?;
    for st in ast.into_iter().filter(|x| matches!(x, sqlparser::ast::Statement::CreateTable(_))) {
        if let sqlparser::ast::Statement::CreateTable(ct) = st {
            let table = ct.name.0[0].as_ident().unwrap().value.to_string();
            let data_types: TableDataTypes = Rc::new(
                HashMap::from_iter(
                    ct.columns.iter().map(|column| (column.name.value.to_string(), column.data_type.to_owned())),
                ),
            );
            return Ok(Some((table, data_types)));
        }
    }
    Ok(None)
}

fn is_insert(statement: &str) -> bool {
    statement.starts_with("INSERT")
}

fn is_create_table(statement: &str) -> bool {
    statement.starts_with("CREATE TABLE")
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

impl<'a> From<&'a Value> for &'a str {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Int{ string, .. } => string.as_str(),
            Value::Date{ string, .. } => string.as_str(),
            Value::String{ string, .. } => string.as_str(),
            Value::Null{ string, .. } => string.as_str(),
        }
    }
}

#[derive(Clone)]
#[derive(Debug)]
pub struct InsertStatement {
    statement: String,
    table: String,
    values_part: String,
    data_types: Option<TableDataTypes>,
    positions: Option<TableColumnPositions>,
    value_per_field: Option<HashMap<String, Value>>,
}

impl InsertStatement {
    fn new(statement: &str) -> Result<Self, anyhow::Error> {
        if !statement.starts_with("INSERT") {
            return Err(anyhow::anyhow!("not an insert statement"));
        }
        let (table, _, values_part) = insert_parts(statement)?;
        Ok(Self { statement: statement.to_string(), table, values_part, value_per_field: None, data_types: None, positions: None })
    }

    pub fn get_table(&self) -> &str {
        &self.table
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

impl<'a> TryFrom<&'a SqlStatement> for InsertStatement {
    type Error = anyhow::Error;
    fn try_from(other: &'a SqlStatement) -> Result<InsertStatement, Self::Error> {
        InsertStatement::new(&other.0)
    }
}

impl<'a> TryFrom<&'a mut InsertStatement> for SqlStatement {
    type Error = anyhow::Error;
    fn try_from(other: &'a mut InsertStatement) -> Result<SqlStatement, Self::Error> {
        Ok(new_statement(other.as_string().to_string(), Some(other.get_table().to_owned())))
    }
}

#[derive(Debug)]
pub struct Tracker {
    data_types: DataTypes,
    column_positions: ColumnPositions,
    captured_values: CapturedValues,
    tracked_column_per_key: HashMap<String, String>,
}

impl Tracker {
    fn new(tracked_columns: &[&str]) -> Result<Rc<RefCell<Self>>, anyhow::Error> {
        let (captured_values, tracked_column_per_key) = Tracker::prepare_tracked_columns(tracked_columns)?;
        Ok(Rc::new(RefCell::new(Tracker {
            data_types: HashMap::new(),
            column_positions: HashMap::new(),
            captured_values,
            tracked_column_per_key,
        })))
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

    fn capture_positions(&mut self, statement: &SqlStatement, current_table: &Option<String>) -> EmptyResult {
        if let Some(table) = current_table {
            if !self.column_positions.contains_key(table) && is_insert(&statement.0) {
                let insert_statement = InsertStatement::try_from(statement)?;
                self.column_positions.insert(table.to_string(), Rc::new(insert_statement.get_column_positions()));
            };
        }
        Ok(())
    }

    fn capture_data_types(&mut self, statement: &SqlStatement) -> EmptyResult {
        if is_create_table(&statement.0) {
            if let Some((table, data_types)) = get_data_types(&statement.0)? {
                self.data_types.insert(table.to_string(), data_types);
            }
        }
        Ok(())
    }

    fn capture(&mut self, statement: &SqlStatement, current_table: &Option<String>) -> EmptyResult {
        self.capture_positions(statement, current_table)?;
        self.capture_data_types(statement)?;
        Ok(())
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
                    let key: &str = value.into();
                    set.insert(key.to_string());
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

struct TrackedStatements {
    iter: PlainStatements,
    current_table: Option<String>,
    unlock_next: bool,
    tracker: Rc<RefCell<Tracker>>,
}

impl TrackedStatements {
    fn from_file(sqldump_filepath: &Path, tracker: &TrackerCell, preprocess_file: &Option<&Path>) -> Result<Self, anyhow::Error> {
        let tracker = Rc::clone(tracker);

        if let Some(file) = preprocess_file {
            let statements = TrackedStatements::from_file(file, &tracker, &None)?;
            // consume iterator to populate tracker
            statements.for_each(drop);
        }

        Ok(TrackedStatements {
            iter: PlainStatements::from_file(sqldump_filepath)?,
            current_table: None,
            unlock_next: false,
            tracker,
        })
    }

    fn extract_table(statement: &str) -> Result<&str, anyhow::Error> {
        let Some(captures) = TABLE_DUMP_RE.captures(statement) else {
            return Err(anyhow::anyhow!("cannot extract table"));
        };

        let Some(captured) = captures.get(1) else {
            return Err(anyhow::anyhow!("cannot extract table"));
        };

        Ok(captured.as_str())
    }

    fn read_statement(&mut self) -> Option<SqlStatementResult> {
        let next = self.iter.next()?;

        if self.unlock_next {
            self.current_table = None;
            self.unlock_next = false;
        } else if next.starts_with("-- Dumping data for table") {
            let Ok(table) = TrackedStatements::extract_table(&next) else {
                return Some(Err(anyhow::anyhow!("cannot extract table")));
            };
            self.current_table = Some(table.to_owned());
        }

        if next.starts_with("UNLOCK TABLES;") {
            self.unlock_next = true;
        }

        Some(Ok(new_statement(next.to_string(), self.current_table.to_owned())))
    }
}

impl Iterator for TrackedStatements {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut statement = self.read_statement()?;

        if let Ok(st) = &mut statement {
            if let Err(e) = self.tracker.borrow_mut().capture(st, &self.current_table) {
                return Some(Err(e));
            }
        }

        Some(statement)
    }
}

struct TransformedStatements<F: TransformFn> {
    iter: TrackedStatements,
    transform: F,
}

impl<F: TransformFn> TransformedStatements<F> {
    fn from_file(sqldump_filepath: &Path, tracked_columns: &[&str], transform: F, preprocess_file: &Option<&Path>) -> Result<Self, anyhow::Error> {
        let tracker = Tracker::new(tracked_columns)?;
        Ok(TransformedStatements {
            iter: TrackedStatements::from_file(sqldump_filepath, &tracker, preprocess_file)?,
            transform,
        })
    }

    fn try_share_meta(&self, insert_statement: &mut InsertStatement) -> EmptyResult {
        let borrowed = self.iter.tracker.borrow();
        let positions = borrowed.get_table_column_positions(&insert_statement.table);
        let data_types = borrowed.get_table_data_types(&insert_statement.table);
        insert_statement.set_meta(positions, data_types);
        Ok(())
    }

    fn try_capture_values(&mut self, insert_statement: &mut InsertStatement) -> EmptyResult {
        let mut borrowed = self.iter.tracker.borrow_mut();
        if borrowed.is_capturing_columns() {
            let value_per_field = insert_statement.get_values()?;
            borrowed.capture_values(value_per_field);
        }
        Ok(())
    }

    fn transform_insert_statement(&mut self, insert_statement: &mut InsertStatement) -> Result<SqlStatement, anyhow::Error>
        where F: TransformFn
    {
        if self.try_share_meta(insert_statement).is_err() {
            return Err(anyhow::anyhow!("cannot share meta"));
        }
        let transformed = (self.transform)(insert_statement)?;
        if transformed.is_some() {
            self.try_capture_values(insert_statement)?;
        }
        let statement: SqlStatement = SqlStatement::try_from(insert_statement)?;
        Ok(statement)
    }

    fn transform_iteration_item(&mut self, item: IteratorItem) -> Option<IteratorItem> {
        match item {
            Err(e) => Some(Err(e)),
            Ok(ref st) => {
                if !is_insert(&st.0) {
                    return Some(item);
                }
                match InsertStatement::try_from(st) {
                    Err(e) => Some(Err(e)),
                    Ok(ref mut insert_statement) => {
                        match self.transform_insert_statement(insert_statement) {
                            Err(e) => Some(Err(e)),
                            Ok(transformed_statement) => {
                                Some(Ok(transformed_statement))
                            }
                        }
                    }
                }
            }
        }
    }

    fn process_all(self, writers: &mut Writers) -> Result<CapturedValues, anyhow::Error> {
        let tracker = Rc::clone(&self.iter.tracker);
        for st in self {
            let statement = st?;
            writers.write_statement(&statement.1, statement.0.as_bytes())?;
        };
        writers.flush()?;
        Ok(tracker.borrow().get_captured_values().clone())
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

pub fn explode_to_files<F>(
    working_file_path: &Path,
    input_filepath: &Path,
    transform: F,
) -> Result<CapturedValues, anyhow::Error>
  where F: TransformFn
{
    let mut writers = Writers::new(working_file_path, false)?;

    let statements = TransformedStatements::from_file(input_filepath, &[], transform, &None)?;
    let res = statements.process_all(&mut writers)?;

    Ok(res)
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

    let mut writers = Writers::new(working_file_path, true)?;
    let input_filepath = &writers.get_table_file(table)?;

    let statements = TransformedStatements::from_file(input_filepath, tracked_columns, transform, &Some(working_file_path))?;
    let res = statements.process_all(&mut writers)?;

    Ok(res)
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

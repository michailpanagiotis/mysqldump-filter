mod sql_parser;
mod writers;

use lazy_static::lazy_static;
use regex::Regex;
use std::cell::RefCell;
use std::{collections::HashMap, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::scanner::sql_parser::{TableColumnPositions, TableDataTypes, get_column_positions, get_data_types, insert_parts, is_create_table, is_insert, values};
use crate::scanner::writers::Writers;

type TrackerCell = Rc<RefCell<Tracker>>;

type SqlStatement = (String, Option<String>);
type SqlStatementResult = Result<SqlStatement, anyhow::Error>;
type IteratorItem = SqlStatementResult;
type EmptyResult = Result<(), anyhow::Error>;

type ValuesMap = HashMap<String, (String, sqlparser::ast::DataType)>;

pub trait AbstractTransformFn<Iv>: FnMut(Iv) -> Result<Option<Iv>, anyhow::Error>
where
    Iv: IntoIterator + Clone + Extend<(String, String)>,
    ValuesMap: FromIterator<<Iv>::Item>
{}

impl<Iv, T: FnMut(Iv) -> Result<Option<Iv>, anyhow::Error>> AbstractTransformFn<Iv> for T
where
    Iv: IntoIterator + Clone + Extend<(String, String)>,
    ValuesMap: FromIterator<<Iv>::Item>
{}

pub trait TransformFn: AbstractTransformFn<InsertStatement> {}
impl<T: AbstractTransformFn<InsertStatement>> TransformFn for T {}

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Clone)]
#[derive(Debug)]
pub struct InsertStatement {
    statement: String,
    table: String,
    values_part: String,
    data_types: Option<Rc<TableDataTypes>>,
    positions: Option<Rc<TableColumnPositions>>,
    value_per_field: Option<ValuesMap>,
}

impl InsertStatement {
    fn new(statement: &str) -> Result<Self, anyhow::Error> {
        if !is_insert(statement) {
            return Err(anyhow::anyhow!("not an insert statement"));
        }
        let (table, _, values_part) = insert_parts(statement)?;
        Ok(Self { statement: statement.to_string(), table, values_part, value_per_field: None, data_types: None, positions: None })
    }

    pub fn get_table(&self) -> &str {
        &self.table
    }

    fn as_string(&self) -> &str {
        &self.statement
    }

    fn set_meta(&mut self, column_positions: &Rc<TableColumnPositions>, data_types: &Rc<TableDataTypes>) {
        self.positions = Some(Rc::clone(column_positions));
        self.data_types = Some(Rc::clone(data_types));
    }

    fn get_value_array(&self) -> Result<Vec<&str>, anyhow::Error> {
        match values(&self.values_part) {
            Err(_) => Err(anyhow::anyhow!("cannot parse values")),
            Ok((_, values)) => Ok(values)
        }
    }

    fn update(&mut self, field: &str, value: &str) -> Result<(), anyhow::Error> {
        let Some(ref data_types) = self.data_types else {
            return Err(anyhow::anyhow!("statement with no data types"));
        };
        if let Some(ref mut value_per_field) = self.value_per_field {
            value_per_field.insert(field.to_owned(), (value.to_owned(), data_types[field].to_owned()));
        }
        Ok(())
    }
}

impl IntoIterator for InsertStatement {
    type Item = <ValuesMap as IntoIterator>::Item;
    type IntoIter = <ValuesMap as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let Some(ref positions) = self.positions else {
            panic!("statement with no positions");
        };
        let Some(ref data_types) = self.data_types else {
            panic!("statement with no data types");
        };

        let Ok((_, value_array)) = values(&self.values_part) else {
            panic!("cannot parse values");
        };

        let values: ValuesMap = positions
            .iter()
            .map(|(column_name, position)| {
                (column_name.to_owned(), (value_array[*position].to_string(), data_types[column_name].to_owned()))
            })
            .collect();
        values.into_iter()
    }
}

impl Extend<(String, String)> for InsertStatement {
    fn extend<T: IntoIterator<Item=(String, String)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.update(&key, &value);
        }
    }
}

impl<'a> TryFrom<&'a SqlStatement> for InsertStatement {
    type Error = anyhow::Error;
    fn try_from(other: &'a SqlStatement) -> Result<InsertStatement, Self::Error> {
        InsertStatement::new(&other.0)
    }
}

impl From<InsertStatement> for SqlStatement {
    fn from(other: InsertStatement) -> Self {
        (other.as_string().to_string(), Some(other.get_table().to_owned()))
    }
}

#[derive(Debug)]
pub struct Tracker {
    data_types: HashMap<String, Rc<TableDataTypes>>,
    column_positions: HashMap<String, Rc<TableColumnPositions>>,
}

impl Tracker {
    fn new() -> Result<Rc<RefCell<Self>>, anyhow::Error> {
        Ok(Rc::new(RefCell::new(Tracker {
            data_types: HashMap::new(),
            column_positions: HashMap::new(),
        })))
    }

    fn capture_positions(&mut self, statement: &SqlStatement, current_table: &Option<String>) -> EmptyResult {
        if let Some(table) = current_table {
            if !self.column_positions.contains_key(table) && is_insert(&statement.0) {
                let insert_statement = InsertStatement::try_from(statement)?;
                self.column_positions.insert(table.to_string(), Rc::new(get_column_positions(&insert_statement.statement)?));
            };
        }
        Ok(())
    }

    fn capture_data_types(&mut self, statement: &SqlStatement) -> EmptyResult {
        if is_create_table(&statement.0) {
            if let Some((table, data_types)) = get_data_types(&statement.0)? {
                self.data_types.insert(table.to_string(), Rc::new(data_types));
            }
        }
        Ok(())
    }

    fn capture(&mut self, statement: &SqlStatement, current_table: &Option<String>) -> EmptyResult {
        self.capture_positions(statement, current_table)?;
        self.capture_data_types(statement)?;
        Ok(())
    }

    fn get_table_data_types(&self, table: &str) -> &Rc<TableDataTypes> {
        &self.data_types[table]
    }

    fn get_table_column_positions(&self, table: &str) -> &Rc<TableColumnPositions> {
        &self.column_positions[table]
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

        Some(Ok((next.to_string(), self.current_table.to_owned())))
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
    fn from_file(sqldump_filepath: &Path, transform: F, preprocess_file: &Option<&Path>) -> Result<Self, anyhow::Error> {
        let tracker = Tracker::new()?;
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

    fn transform_insert_statement(&mut self, mut insert_statement: InsertStatement) -> Result<Option<InsertStatement>, anyhow::Error>
        where F: TransformFn
    {
        if self.try_share_meta(&mut insert_statement).is_err() {
            return Err(anyhow::anyhow!("cannot share meta"));
        }
        let transformed = (self.transform)(insert_statement)?;
        Ok(transformed)
    }

    fn transform_iteration_item(&mut self, statement_result: SqlStatementResult) -> Option<SqlStatementResult> {
        let Ok(ref statement) = statement_result else { return Some(statement_result); };
        let Ok(insert_statement): Result<InsertStatement, anyhow::Error> = statement.try_into() else { return Some(statement_result); };
        let Ok(transformed_insert_statement) = self.transform_insert_statement(insert_statement) else { return Some(statement_result); };
        transformed_insert_statement.map(|x| Ok(x.into()))
    }

    fn process_all(self, writers: &mut Writers) -> Result<(), anyhow::Error> {
        for st in self {
            let statement = st?;
            writers.write_statement(&statement.1, statement.0.as_bytes())?;
        };
        writers.flush()?;
        Ok(())
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
) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    let mut writers = Writers::new(working_file_path, false)?;

    let statements = TransformedStatements::from_file(input_filepath, transform, &None)?;
    statements.process_all(&mut writers)?;

    Ok(())
}

pub fn process_table_inserts<F>(
    working_file_path: &Path,
    table: &str,
    transform: F,
) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    println!("Processing records of table {table}");

    let mut writers = Writers::new(working_file_path, true)?;
    let input_filepath = &writers.get_table_file(table)?;

    let statements = TransformedStatements::from_file(input_filepath, transform, &Some(working_file_path))?;
    statements.process_all(&mut writers)?;

    Ok(())
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

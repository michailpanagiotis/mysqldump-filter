mod sql_parser;
mod writers;

use lazy_static::lazy_static;
use regex::Regex;
use core::panic;
use std::cell::RefCell;
use std::{collections::HashMap, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::scanner::sql_parser::{TableColumnPositions, TableDataTypes, get_column_positions, get_data_types, split_insert_parts, is_create_table, is_insert, values};
use crate::scanner::writers::{Writers, get_table_file};

type DBMetaCell = Rc<RefCell<DBMeta>>;

type SqlStatementResult = Result<SqlStatement, anyhow::Error>;
type IteratorItem = SqlStatementResult;
type EmptyResult = Result<(), anyhow::Error>;

type ValuesMap = HashMap<String, (String, sqlparser::ast::DataType)>;

pub trait AbstractTransformFn<Iv>: FnMut(Iv) -> Result<Option<Iv>, anyhow::Error>
where
    Iv: IntoIterator + Clone + for<'a> Extend<(&'a String, &'a String)>,
    ValuesMap: FromIterator<<Iv>::Item>
{}

impl<Iv, T: FnMut(Iv) -> Result<Option<Iv>, anyhow::Error>> AbstractTransformFn<Iv> for T
where
    Iv: IntoIterator + Clone + for<'a> Extend<(&'a String, &'a String)>,
    ValuesMap: FromIterator<<Iv>::Item>
{}

pub trait TransformFn: AbstractTransformFn<SqlStatement> {}
impl<T: AbstractTransformFn<SqlStatement>> TransformFn for T {}

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Clone)]
#[derive(Debug)]
pub struct SqlStatement {
    text: String,
    table: Option<String>,
    db_meta: Option<DBMetaCell>,
}

impl SqlStatement {
    pub fn get_table(&self) -> &Option<String> {
        &self.table
    }

    fn set_meta(&mut self, db_meta_cell: &DBMetaCell) {
        self.db_meta = Some(Rc::clone(db_meta_cell));
    }

    fn get_insert_parts(&self) -> Option<(String, String, Vec<String>)> {
        if !is_insert(&self.text) {
            return None;
        }

        let Ok((table, columns_part, values_part)) = split_insert_parts(&self.text) else {
            panic!("cannot split insert parts");
        };

        let Ok((_, value_array)) = values(&values_part) else {
            panic!("cannot parse values");
        };

        Some((table, columns_part, value_array.iter().map(|x| x.to_string()).collect()))
    }
}

impl IntoIterator for SqlStatement {
    type Item = <ValuesMap as IntoIterator>::Item;
    type IntoIter = <ValuesMap as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let Some((table, _, value_array)) = self.get_insert_parts() else {
            return ValuesMap::default().into_iter();
        };

        let Some(ref meta) = self.db_meta else {
            panic!("statement with no meta");
        };
        let binding = meta.borrow();
        let Some(data_types) = binding.data_types.get(&table) else {
            panic!("statement with no data types");
        };

        let Some(positions) = binding.column_positions.get(&table) else {
            panic!("statement with no positions");
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

impl<'a> Extend<(&'a String, &'a String)> for SqlStatement {
    fn extend<T: IntoIterator<Item=(&'a String, &'a String)>>(&mut self, iter: T) {
        if let Some((table, columns_part, mut values)) = self.get_insert_parts() {
            let Some(ref meta) = self.db_meta else {
                panic!("statement with no meta");
            };
            let binding = meta.borrow();
            let Some(positions) = binding.column_positions.get(&table) else {
                panic!("statement with no positions");
            };

            for (field, value) in iter {
                values[positions[field]] = value.to_string();
            }
            self.text = format!("INSERT INTO `{}` ({}) VALUES ({});\n", table, columns_part, values.join(","));
        }
    }
}

#[derive(Debug)]
pub struct DBMeta {
    data_types: HashMap<String, Rc<TableDataTypes>>,
    column_positions: HashMap<String, Rc<TableColumnPositions>>,
}

impl DBMeta {
    fn from_file(filename: &Path) -> Result<Rc<RefCell<Self>>, anyhow::Error> {
        let db_meta = DBMeta::new()?;
        let statements = TrackedStatements::from_file(filename, Some(&db_meta))?;
        // consume iterator to populate db_meta
        statements.for_each(drop);
        Ok(db_meta)
    }

    fn new() -> Result<DBMetaCell, anyhow::Error> {
        Ok(Rc::new(RefCell::new(DBMeta {
            data_types: HashMap::new(),
            column_positions: HashMap::new(),
        })))
    }

    fn capture(&mut self, statement: &SqlStatement) -> EmptyResult {
        if is_create_table(&statement.text) {
            if let Some((table, data_types)) = get_data_types(&statement.text)? {
                self.data_types.insert(table.to_string(), Rc::new(data_types));
            }
        }
        if let Some(ref table) = statement.table {
            if !self.column_positions.contains_key(table) && is_insert(&statement.text) {
                self.column_positions.insert(table.to_string(), Rc::new(get_column_positions(&statement.text)?));
            };
        }
        Ok(())
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
    db_meta: DBMetaCell,
}

impl TrackedStatements {
    fn from_file(sqldump_filepath: &Path, db_meta: Option<&DBMetaCell>) -> Result<Self, anyhow::Error> {
        let db_meta = if let Some(db_meta) = db_meta { Rc::clone(db_meta) } else { DBMeta::new()? };
        Ok(TrackedStatements {
            iter: PlainStatements::from_file(sqldump_filepath)?,
            current_table: None,
            unlock_next: false,
            db_meta,
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
            println!("Processing table {table}");
            self.current_table = Some(table.to_owned());
        }

        if next.starts_with("UNLOCK TABLES;") {
            self.unlock_next = true;
        }

        Some(Ok(SqlStatement{ text: next.to_string(), table: self.current_table.to_owned(), db_meta: None }))
    }
}

impl Iterator for TrackedStatements {
    type Item = IteratorItem;
    fn next(&mut self) -> Option<IteratorItem> {
        let mut statement = self.read_statement()?;

        if let Ok(st) = &mut statement {
            if let Err(e) = self.db_meta.borrow_mut().capture(st) {
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
    fn from_file(sqldump_filepath: &Path, transform: F, db_meta: Option<&DBMetaCell>) -> Result<Self, anyhow::Error> {
        Ok(TransformedStatements {
            iter: TrackedStatements::from_file(sqldump_filepath, db_meta)?,
            transform,
        })
    }

    fn transform_iteration_item(&mut self, statement_result: SqlStatementResult) -> Option<SqlStatementResult> {
        let Ok(mut statement) = statement_result else { return Some(statement_result); };
        statement.set_meta(&self.iter.db_meta);
        let tr: Option<SqlStatement> = (self.transform)(statement).expect("err");
        tr.map(Ok)
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

pub fn process<F>(working_file_path: &Path, input_filepath: &Path, transform: F, db_meta: Option<DBMetaCell>,) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    let mut writers = Writers::new(working_file_path)?;
    for st in TransformedStatements::from_file(input_filepath, transform, db_meta.as_ref())? {
        let statement = st?;
        writers.write_statement(&statement.table, statement.text.as_bytes())?;
    };
    writers.flush()?;

    Ok(())
}

pub fn explode_to_files<F>(
    working_file_path: &Path,
    input_filepath: &Path,
    transform: F,
) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    process(working_file_path, input_filepath, transform, None)
}

pub fn process_table_inserts<F>(
    working_file_path: &Path,
    table: &str,
    transform: F,
) -> Result<(), anyhow::Error>
  where F: TransformFn
{
    println!("Processing records of table {table}");
    let input_filepath = &get_table_file(working_file_path, table)?;

    process(working_file_path, input_filepath, transform, Some(DBMeta::from_file(working_file_path)?))
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

use lazy_static::lazy_static;
use regex::Regex;
use std::cell::RefCell;
use std::{collections::HashMap, fs::File};
use std::fs;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::sql::{get_columns, parse_insert_parts};

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

pub fn process_table_file<F>(
    working_file_path: &Path,
    table: &str,
    mut transform: F,
) -> Result<(), anyhow::Error>
  where F: FnMut(&SqlStatement, &Tracker) -> Result<Option<SqlStatement>, anyhow::Error>
{
    let tracker = Tracker::from_working_file_path(working_file_path)?;
    let table_file = tracker.get_table_file(table);
    let output_file = &table_file.with_extension("proc");
    let mut writer = get_writer(output_file)?;

    let statements = TrackedStatements::from_file(table_file, &Some(&Rc::new(RefCell::new(tracker.clone()))))?;

    for (st, tr_option) in statements {
        let tr = tr_option.ok_or(anyhow::anyhow!("unknown tracker"))?;
        let input_statement = st?;
        let transformed = transform(&input_statement, &tr.borrow())?;
        if let Some(statement) = transformed {
            writer.write_all(&statement.as_bytes())?;
        }
    };

    fs::rename(output_file, table_file).expect("cannot rename");

    Ok(())
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

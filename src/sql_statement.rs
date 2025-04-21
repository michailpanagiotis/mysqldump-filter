use lazy_static::lazy_static;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::parser::{parse_insert_fields, parse_insert_values};
use crate::config::TableConfig;
use crate::trackers::{InsertTracker, ReferenceTracker};
use crate::io_utils::read_file;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Debug)]
#[derive(Clone)]
pub struct FieldPositions(HashMap<String, usize>);

impl FieldPositions {
    fn new(insert_statement: &str) -> Self {
        let fields = parse_insert_fields(insert_statement);
        FieldPositions(HashMap::from_iter(
            fields.iter().enumerate().map(|(idx, item)| (item.to_string(), idx))
        ))
    }

    fn get_position(&self, field: &str) -> usize {
        self.0[field]
    }

    pub fn get_value(&self, statement: &Statement, field: &String) -> String {
        let values = statement.get_all_values();
        let position = self.0[field];
        values[position].to_string()
    }

    pub fn get_values(&self, statement: &Statement, fields: &HashSet<String>) -> HashMap<String, String> {
        let values = statement.get_all_values();

        let value_per_field: HashMap<String, String> = HashMap::from_iter(fields.iter().map(|f| {
            let position = self.get_position(f);
            (f.clone(), values[position].to_string())
        }));

        value_per_field
    }

    pub fn filtered(&mut self, fields: &HashSet<String>) -> Self {
        FieldPositions(HashMap::from_iter(
            self.0.iter()
                .filter(|(key, _)| fields.contains(*key))
                .map(|(key, value)| (key.clone(), *value))
        ))
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
#[derive(Clone)]
enum StatementType {
    Unknown,
    Insert,
}

#[derive(Debug)]
#[derive(Clone)]
pub struct Statement {
    line: String,
    table: Option<String>,
    r#type: StatementType,
}

impl Statement {
    pub fn new(table: &Option<String>, line: &str) -> Self {
       let statement_type = if line.starts_with("INSERT") { StatementType::Insert } else { StatementType::Unknown };
       Statement {
        line: line.to_string(),
        r#type: statement_type,
        table: table.clone(),
       }
    }

    pub fn from_file<'a>(sqldump_filepath: &'a Path, requested_tables: &HashSet<String>) -> impl Iterator<Item = Statement> + use<'a> {
        let valid_tables = requested_tables.clone();

        let mut current_table: Option<String> = None;
        let to_statement = move |line: String| {
            if line.starts_with("-- Dumping data for table") {
                current_table = Some(
                    TABLE_DUMP_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string(),
                );
            }
            if current_table.as_ref().is_some_and(|t| !valid_tables.contains(t)) {
                return None;
            }
            Some(Statement::new(&current_table, &line))
        };

        read_file(sqldump_filepath).flat_map(to_statement)
    }

    pub fn is_insert(&self) -> bool {
        self.r#type == StatementType::Insert
    }

    pub fn get_table(&self) -> Option<String> {
        self.table.clone()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.line.as_bytes()
    }

    pub fn get_field_positions(&self, fields: &HashSet<String>) -> Option<FieldPositions> {
        if !self.is_insert() {
            return None;
        }
        Some(FieldPositions::new(&self.line).filtered(fields))
    }

    pub fn get_all_values(&self) -> Vec<&str> {
        let values = parse_insert_values(&self.line);
        values
    }
}

pub struct TableStatementsIterator<I: Iterator<Item=Statement>> {
    inner: I,
    insert_tracker: Option<InsertTracker>,
    ref_tracker: Option<ReferenceTracker>,
}

impl<I: Iterator<Item=Statement>> Iterator for TableStatementsIterator<I> {
    type Item = Statement;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.inner.next();
        while let Some(ref x) = next {
            if self.should_keep_item(x) {
                break;
            }
            next = self.inner.next();
        }

        next
    }
}

impl<I: Iterator<Item=Statement>> TableStatementsIterator<I> {
    pub fn new(
        table_config: &TableConfig,
        statements: I,
    ) -> Self
    {
        let ref_tracker = table_config.get_reference_tracker();
        let insert_tracker = table_config.get_insert_tracker();
        TableStatementsIterator {
            insert_tracker,
            ref_tracker,
            inner: statements,
        }
    }

    fn should_keep_item(&mut self, statement: &Statement) -> bool {
        let Some(info) = &mut self.insert_tracker else { return true };
        info.should_keep_statement(statement)
    }
}

pub fn scan_statements<I: Iterator<Item=Statement>>(
    table_config: &TableConfig,
    working_dir: &Path,
    default: &Path,
    statements: I,
) -> (Option<ReferenceTracker>, PathBuf) {
    let mut writer = table_config.get_writer(working_dir, default);
    if let Some(table) = &table_config.table {
        println!("Parsing table {}", &table);
    }
    let mut ref_tracker = table_config.get_reference_tracker();
    for statement in statements {
        if let Some(ref mut tracker) = ref_tracker {
            tracker.capture(&statement);
        }
        writer.write_statement(&statement).expect("Unable to write data");
    };
    writer.flush().expect("Cannot flush buffer");

    (ref_tracker, writer.get_filepath())
}

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::expression_parser::{get_table_from_comment, parse_insert_fields, parse_insert_values};
use crate::filters::TableFilters;
use crate::io_utils::read_file;

#[derive(Debug)]
#[derive(Clone)]
pub struct FieldPositions(HashMap<String, usize>);

impl FieldPositions {
    fn new(insert_statement: &str) -> Self {
        FieldPositions(parse_insert_fields(insert_statement))
    }

    pub fn get_value(&self, statement: &Statement, field: &String) -> String {
        let values = statement.get_all_values();
        let position = self.0[field];
        values[position].to_string()
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
            if let Some(t) = get_table_from_comment(&line) {
                current_table = Some(t);
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

    pub fn as_str(&self) -> &str {
        self.line.as_str()
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

pub struct TableStatementsIterator<'a, I: Iterator<Item=Statement>> {
    inner: I,
    filters: TableFilters,
    references: Option<&'a HashMap<String, HashSet<String>>>,
}

impl<I: Iterator<Item=Statement>> Iterator for TableStatementsIterator<'_, I> {
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

impl<'a, I: Iterator<Item=Statement>> TableStatementsIterator<'a, I> {
    pub fn new(
        filters: &TableFilters,
        references: Option<&'a HashMap<String, HashSet<String>>>,
        statements: I,
    ) -> Self
    {
        TableStatementsIterator {
            filters: filters.clone(),
            references,
            inner: statements,
        }
    }

    fn should_keep_item(&mut self, statement: &Statement) -> bool {
        if !statement.is_insert() || statement.get_table().is_none() {
            return true;
        }
        self.filters.test_values(statement.as_str(), &self.references)
    }
}

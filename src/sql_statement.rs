use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::expression_parser::get_table_from_comment;
use crate::filters::TableFilters;
use crate::io_utils::read_file;

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
}

pub struct TableStatementsIterator<'a, 'b, I: Iterator<Item=Statement>> {
    inner: I,
    filters: &'a mut TableFilters,
    references: Option<&'b HashMap<String, HashSet<String>>>,
}

impl<I: Iterator<Item=Statement>> Iterator for TableStatementsIterator<'_, '_, I> {
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

impl<'a, 'b, I: Iterator<Item=Statement>> TableStatementsIterator<'a, 'b, I> {
    pub fn new(
        filters: &'a mut TableFilters,
        references: Option<&'b HashMap<String, HashSet<String>>>,
        statements: I,
    ) -> Self
    {
        TableStatementsIterator {
            filters,
            references,
            inner: statements,
        }
    }

    fn should_keep_item(&mut self, statement: &Statement) -> bool {
        if !statement.is_insert() || statement.get_table().is_none() {
            return true;
        }
        self.filters.test_insert_statement(statement.as_str(), &self.references)
    }
}

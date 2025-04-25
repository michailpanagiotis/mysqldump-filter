use std::collections::HashSet;
use std::path::Path;

use crate::expression_parser::get_table_from_comment;
use crate::io_utils::read_file;

#[derive(Debug)]
#[derive(Clone)]
pub struct Statement {
    pub line: String,
    pub table: Option<String>,
}

impl Statement {
    pub fn new(table: &Option<String>, line: &str) -> Self {
       Statement {
        line: line.to_string(),
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
}

use std::collections::{HashMap, HashSet};
use std::cell::RefCell;

use crate::checks::{ColumnMeta, RowCheck, TestValue, ValueTest};
use crate::sql::{get_column_positions, get_values};
use crate::references::References;

#[derive(Debug)]
pub struct FilterConditions {
    per_table: HashMap<String, RowCheck>,
    all_filtered_tables: HashSet<String>,
    pub pending_tables: HashSet<String>,
    pub fully_filtered_tables: HashMap<String, usize>,
    pub current_pass: usize,
}

impl FilterConditions {
    pub fn new(per_table: HashMap<String, RowCheck>) -> Self {
        FilterConditions {
            per_table,
            all_filtered_tables: HashSet::new(),
            pending_tables: HashSet::new(),
            fully_filtered_tables: HashMap::new(),
            current_pass: 0,
        }
    }

    fn has_table_conditions(&self, table: &str) -> bool {
        self.per_table.contains_key(table) && !self.per_table[table].is_empty()
    }

    pub fn track_filtered(&mut self, table: &str) {
        if !self.fully_filtered_tables.contains_key(table) {
            let dependencies = match self.per_table.get(table) {
                Some(x) => x.get_table_dependencies(),
                None => HashSet::new(),
            };
            for dependency in &dependencies {
                if !self.fully_filtered_tables.contains_key(dependency) {
                    self.pending_tables.insert(table.to_owned());
                    return;
                }
            }

            self.pending_tables.remove(table);

            let last_pass: Option<usize> = dependencies.iter().map(|d| self.fully_filtered_tables[d]).max();
            self.fully_filtered_tables.insert(table.to_owned(), self.current_pass);
        }
    }

    pub fn test_sql_statement(
        &mut self,
        sql_statement: &str,
        table: &str,
        lookup_table: &Option<HashMap<String, HashSet<String>>>,
    ) -> bool {
        if !sql_statement.starts_with("INSERT") {
            return true;
        }

        if self.fully_filtered_tables.get(table).is_some_and(|x| x < &self.current_pass) {
            return true;
        }

        self.track_filtered(table);

        if !self.has_table_conditions(table) {
            return true;
        }

        self.per_table.get_mut(table).expect("cannot find tests for table").test(sql_statement, lookup_table)
    }

    pub fn filter<I: Iterator<Item=(Option<String>, String)>>(&mut self, statements: I, references: &mut References) -> impl Iterator<Item=(Option<String>, String)> {
        self.current_pass += 1;
        let lookup = if references.is_empty() { None } else {
            let lookup = references.get_lookup_table();
            dbg!(&self.fully_filtered_tables);
            dbg!(&self.pending_tables);
            references.clear();
            Some(lookup)
        };
        statements.filter(move |(table_option, statement)| {
            let Some(table) = table_option else { return true };
            let should_keep = self.test_sql_statement(statement, table, &lookup);
            if should_keep {
                references.capture(table, statement);
            }
            should_keep
        })
    }
}

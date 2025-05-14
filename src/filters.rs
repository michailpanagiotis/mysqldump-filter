use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::checks::{ValueTest, TestValue};
use crate::sql::{get_column_positions, get_values};
use crate::references::References;

#[derive(Debug)]
pub struct FilterConditions {
    pub inner: HashMap<String, HashMap<String, Vec<ValueTest>>>,
    all_filtered_tables: HashSet<String>,
    pub pending_tables: HashSet<String>,
    pub fully_filtered_tables: HashMap<String, usize>,
    pub current_pass: usize,
}

impl FilterConditions {
    pub fn new(collected: Vec<ValueTest>) -> Self {
        FilterConditions {
            inner: collected.into_iter()
                .chunk_by(|x| x.get_table_name().to_owned())
                .into_iter()
                .map(|(table, conds)| (table, conds.into_iter().into_group_map_by(|x| x.get_column_name().to_owned()))).collect(),
            all_filtered_tables: HashSet::new(),
            pending_tables: HashSet::new(),
            fully_filtered_tables: HashMap::new(),
            current_pass: 0,
        }
    }

    fn has_resolved_positions(&self, table: &str) -> bool {
        self.inner[table].values().flatten().all(|condition| {
            condition.has_resolved_position()
        })
    }

    fn resolve_positions(&mut self, table: &str, insert_statement: &str) {
        let positions: HashMap<String, usize> = get_column_positions(insert_statement);
        for condition in self.inner.get_mut(table).expect("unknown table").values_mut().flatten() {
            match positions.get(condition.get_column_name()) {
                Some(pos) => condition.set_position(*pos),
                None => panic!("{}", format!("unknown column {}", condition.get_column_name())),
            }
        }
        assert!(self.has_resolved_positions(table));
    }

    pub fn get_table_dependencies(&self, table: &str) -> HashSet<String> {
        let mut dependencies = HashSet::new();
        if !self.inner.contains_key(table) || self.inner[table].is_empty() {
            return dependencies;
        }

        for condition in self.inner[table].values().flatten() {
            if let ValueTest::Cascade(t) = condition {
                dependencies.insert(t.get_target_table());
            }
        }
        dependencies
    }

    pub fn can_table_be_fully_filtered(&self, table: &str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        if !self.inner.contains_key(table) || self.inner[table].is_empty() {
            return false;
        }
        for condition in self.inner[table].values().flatten() {
            if let ValueTest::Cascade(t) = condition {
                let Some(l) = lookup_table else {
                    return false;
                };
                if !l.contains_key(t.get_column_key()) {
                    return false;
                }
            }
        }
        return true;
    }

    pub fn track_filtered(&mut self, table: &str) {
        if !self.fully_filtered_tables.contains_key(table) {
            let dependencies = self.get_table_dependencies(table);
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

        if !self.inner.contains_key(table) || self.inner[table].is_empty() {
            return true;
        }

        if !self.has_resolved_positions(table) {
            self.resolve_positions(table, sql_statement);
        }

        let values = get_values(sql_statement);

        if !self.inner[table].values().flatten().all(|condition| {
            condition.get_column_position().is_some_and(|p| condition.test(values[p], lookup_table))
        }) {
            return false;
        }

        true
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

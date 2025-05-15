use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::rc::{Rc, Weak};
use std::cell::RefCell;

use crate::checks::{ValueTest, TestValue};
use crate::sql::{get_column_positions, get_values};
use crate::references::References;

#[derive(Debug)]
pub struct FilterConditions {
    collected: Vec<Rc<RefCell<ValueTest>>>,
    pub inner: HashMap<String, HashMap<String, Vec<Rc<RefCell<ValueTest>>>>>,
    all_filtered_tables: HashSet<String>,
    pub pending_tables: HashSet<String>,
    pub fully_filtered_tables: HashMap<String, usize>,
    pub current_pass: usize,
}

impl FilterConditions {
    pub fn new(collected: Vec<ValueTest>) -> Self {
        let collected: Vec<Rc<RefCell<ValueTest>>> = collected.into_iter().map(|x| Rc::new(RefCell::new(x))).collect();
        let inner: HashMap<String, HashMap<String, Vec<Rc<RefCell<ValueTest>>>>> = collected.iter()
                .chunk_by(|x| x.borrow().get_table_name().to_owned())
                .into_iter()
                .map(|(table, conds)| (table, conds.into_iter().map(|x| {
                    let res: Rc<RefCell<ValueTest>> = Rc::clone(&x);
                    res
                }).into_group_map_by(|x| x.borrow().get_column_name().to_owned()))).collect();

        FilterConditions {
            collected,
            inner,
            all_filtered_tables: HashSet::new(),
            pending_tables: HashSet::new(),
            fully_filtered_tables: HashMap::new(),
            current_pass: 0,
        }
    }

    fn has_table_conditions(&self, table: &str) -> bool {
        self.inner.contains_key(table) && !self.inner[table].is_empty()
    }

    fn get_table_conditions(&self, table: &str) -> impl Iterator<Item=&Rc<RefCell<ValueTest>>> {
        self.inner[table].values().flatten()
    }

    fn has_resolved_positions(&self, table: &str) -> bool {
        self.get_table_conditions(table).all(|condition| {
            condition.borrow().has_resolved_position()
        })
    }

    fn resolve_positions(&mut self, table: &str, insert_statement: &str) {
        let positions: HashMap<String, usize> = get_column_positions(insert_statement);
        for condition in self.get_table_conditions(table) {
            condition.borrow_mut().set_position_from_column_positions(&positions);
        }
        assert!(self.has_resolved_positions(table));
    }

    pub fn get_table_dependencies(&self, table: &str) -> HashSet<String> {
        let mut dependencies = HashSet::new();
        if !self.has_table_conditions(table) {
            return dependencies;
        }

        for condition in self.get_table_conditions(table) {
            for dependency in condition.borrow().get_table_dependencies() {
                dependencies.insert(dependency);
            }
        }
        dependencies
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

        if !self.has_table_conditions(table) {
            return true;
        }


        let values = get_values(sql_statement);

        if !self.has_resolved_positions(table) {
            self.resolve_positions(table, sql_statement);
        }

        if !self.get_table_conditions(table).all(|condition| {
            condition.borrow().test_row(&values, lookup_table)
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

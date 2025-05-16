use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::checks::RowCheck;
use crate::references::References;

#[derive(Debug)]
pub struct FilterConditions<'a> {
    per_table: &'a mut HashMap<String, Rc<RefCell<RowCheck>>>,
    pub current_pass: usize,
}

impl<'a> FilterConditions<'a> {
    pub fn new(per_table: &'a mut HashMap<String, Rc<RefCell<RowCheck>>>) -> Self {
        FilterConditions {
            per_table,
            current_pass: 0,
        }
    }

    fn has_table_conditions(&self, table: &str) -> bool {
        self.per_table.contains_key(table) && !self.per_table[table].borrow().is_empty()
    }

    fn get_done_tables(&self) -> HashSet<String> {
        self.per_table.iter().filter(|(_, row_check)| row_check.borrow().has_been_tested()).map(|(table, _)| table.to_owned()).collect()
    }

    fn get_pending_tables(&self) -> HashSet<String> {
        self.per_table.iter().filter(|(_, row_check)| !row_check.borrow().has_been_tested()).map(|(table, _)| table.to_owned()).collect()
    }

    fn get_ready_tables(&self) -> HashSet<String> {
        self.per_table.iter().filter(|(_, row_check)| row_check.borrow().is_ready_to_be_tested()).map(|(table, _)| table.to_owned()).collect()
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

        if !self.has_table_conditions(table) {
            return true;
        }

        self.per_table.get_mut(table).expect("cannot find tests for table").borrow_mut().test(&self.current_pass, sql_statement, lookup_table)
    }

    pub fn filter<I: Iterator<Item=(Option<String>, String)>>(&mut self, statements: I, references: &mut References) -> impl Iterator<Item=(Option<String>, String)> {
        self.current_pass += 1;
        dbg!(&self.per_table);
        dbg!(self.get_done_tables());
        dbg!(self.get_pending_tables());
        let ready_tables = self.get_ready_tables();
        dbg!(&ready_tables);
        let lookup = if references.is_empty() { None } else {
            let lookup = references.get_lookup_table();
            references.clear();
            Some(lookup)
        };
        statements.filter(move |(table_option, statement)| {
            let Some(table) = table_option else { return true };
            if !ready_tables.contains(table) { return true };

            let should_keep = self.test_sql_statement(statement, table, &lookup);
            if should_keep {
                references.capture(table, statement);
            }
            should_keep
        })
    }
}

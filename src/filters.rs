use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::checks::RowCheck;
use crate::traits::ReferenceTracker;

#[derive(Debug)]
pub struct FilterConditions<'a> {
    per_table: &'a mut HashMap<String, Rc<RefCell<RowCheck<'a>>>>,
    pub current_pass: usize,
}

impl<'a> FilterConditions<'a> {
    pub fn new(per_table: &'a mut HashMap<String, Rc<RefCell<RowCheck<'a>>>>) -> Self {
        FilterConditions {
            per_table,
            current_pass: 0,
        }
    }

    fn get_current_loookup(&self) -> HashMap<String, HashSet<String>> {
        let mut lookup: HashMap<String, HashSet<String>> = HashMap::new();

        for (_, row_check) in self.per_table.iter() {
            lookup.extend(row_check.borrow().get_references().iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
        }

        lookup
    }

    pub fn test_sql_statement(
        &mut self,
        sql_statement: &str,
        table_option: &Option<String>,
        lookup_table: &HashMap<String, HashSet<String>>,
    ) -> Result<bool, anyhow::Error> {
        let Some(table) = table_option else { return Ok(true) };

        if !sql_statement.starts_with("INSERT") {
            return Ok(true);
        }

        if !self.per_table.contains_key(table) {
            return Ok(true);
        }

        self.per_table.get_mut(table).expect("cannot find tests for table").borrow_mut().test(&self.current_pass, sql_statement, lookup_table)
    }

    pub fn filter<I: Iterator<Item=(Option<String>, String)>>(&mut self, statements: I) -> impl Iterator<Item=(Option<String>, String)> {
        self.current_pass += 1;
        let lookup = self.get_current_loookup();

        statements.filter(move |(table_option, statement)| {
            let passed = self.test_sql_statement(statement, table_option, &lookup);
            if passed.is_err() {
                panic!("{}", &passed.unwrap_err());
            }
            !(passed.is_ok_and(|p| !p))
        })
    }
}

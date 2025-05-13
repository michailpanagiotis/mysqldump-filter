use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::filters::{FieldCondition, LookupTest, Tests};
use crate::sql::{get_field_positions, get_values};

#[derive(Debug)]
pub struct References {
    fields: HashMap<String, HashSet<(String, String, String)>>,
    values_per_field: HashMap<String, HashSet<String>>,
    position_per_field: HashMap<String, HashMap<String, usize>>,
}

impl References {
    pub fn new<'a>(conditions: &'a [FieldCondition]) -> Self {
        let lookup_tests: Vec<&'a LookupTest> = conditions
            .iter()
            .flat_map(|fc| match &fc.test {
                Tests::Cascade(cond) => Some(cond),
                _ => None,
            })
            .collect();

        let values_per_field = HashMap::from_iter(lookup_tests.iter().map(|x| (x.get_key(), HashSet::new())));

        let fields: HashMap<String, HashSet<(String, String, String)>> = lookup_tests.iter()
            .map(|cond| {
                let (table, column) = cond.get_foreign_key();
                (table.to_owned(), column.to_owned(), cond.get_key())
            })
            .into_grouping_map_by(|(table, _, _)| table.clone())
            .collect();

        References {
            fields,
            values_per_field,
            position_per_field: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values_per_field.values().all(|x| x.is_empty())
    }

    pub fn get_lookup_table(&self) -> HashMap<String, HashSet<String>> {
        self.values_per_field.clone()
    }

    fn has_referenced_fields(&self, table: &str) -> bool {
        self.fields.contains_key(table)
    }

    fn has_resolved_positions(&self, table: &str) -> bool {
        self.position_per_field.contains_key(table)
    }

    fn resolve_positions(&mut self, table: &str, insert_statement: &str) {
        self.position_per_field.insert(table.to_owned(), get_field_positions(insert_statement));
        assert!(self.has_resolved_positions(table));
    }

    pub fn capture(&mut self, table: &str, insert_statement: &str) {
        if !insert_statement.starts_with("INSERT") {
            return;
        }

        if !self.has_referenced_fields(table) {
            return;
        }

        if !self.has_resolved_positions(table) {
            self.resolve_positions(table, insert_statement);
        }

        let curr_values = get_values(insert_statement);

        for (_, field, key) in self.fields[table].iter() {
            let pos = self.position_per_field[table][field];
            let values = self.values_per_field.get_mut(key).expect("cannot find values lookup");
            values.insert(curr_values[pos].to_string());
        }
    }

    pub fn clear(&mut self) {
        self.values_per_field.values_mut().for_each(|t| {
            t.clear();
        });
    }
}

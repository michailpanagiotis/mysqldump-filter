use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::filters::{Tests, FieldCondition};
use crate::sql::{get_field_positions, get_values};

#[derive(Debug)]
pub struct References {
    fields: HashMap<String, HashSet<(String, String)>>,
    values: HashMap<String, HashSet<String>>,
    position_per_field: HashMap<String, HashMap<String, usize>>,
}

impl References {
    pub fn new<'a, I: IntoIterator<Item = &'a FieldCondition>>(iter: I) -> Self {
        let cascade_conditions: Vec<&'a FieldCondition> = iter.into_iter()
            .filter(|fc| matches!(fc.test, Tests::Cascade(_))).collect();

        let all_fields: HashSet<String> = cascade_conditions.iter().map(|fc| {
            let Tests::Cascade(ref cond) = fc.test else { panic!("test is not a cascade") };
            let (table, column) = cond.get_foreign_key();
            table.to_owned() + "." + column.as_str()
        }).collect();

        let fields: HashMap<String, HashSet<(String, String)>> = cascade_conditions.iter().map(|fc| {
            let Tests::Cascade(ref cond) = fc.test else { panic!("test is not a cascade") };
            let (table, column) = cond.get_foreign_key();
            (table.to_owned(), column.to_owned())
        })
            .into_grouping_map_by(|(table, _)|
                table.clone()
            )
            .collect();

        References {
            fields,
            values: HashMap::from_iter(all_fields.iter().map(|x| (x.to_owned(), HashSet::new()))),
            position_per_field: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.values().all(|x| x.is_empty())
    }

    pub fn get_lookup_table(&self) -> HashMap<String, HashSet<String>> {
        self.values.clone()
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

        for (_, field) in self.fields[table].iter() {
            let pos = self.position_per_field[table][field];
            let key = table.to_owned() + "." + field;
            let values = self.values.get_mut(&key).expect("cannot find values lookup");
            values.insert(curr_values[pos].to_string());
        }
    }

    pub fn clear(&mut self) {
        self.values.values_mut().for_each(|t| {
            t.clear();
        });
    }
}

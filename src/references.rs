use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::filters::{Tests, FieldCondition};
use crate::sql::{get_field_positions, get_values};

#[derive(Debug)]
struct TableReferences {
    table: String,
    values_per_field: HashMap<String, HashSet<String>>,
    position_per_field: HashMap<String, usize>,
}

impl TableReferences {
    fn new<T: IntoIterator<Item=(String, String)>>(iter: T) -> Self {
        let fields: Vec<(String, String)> = iter.into_iter().collect();

        let distinct: Vec<&(String, String)> = fields.iter().unique_by(|(table, _)| table.clone()).collect();
        if distinct.len() != 1 {
            panic!("fields have different tables");
        }
        let table = distinct[0].0.to_string();
        let values_per_field = fields.clone().into_iter().map(|(_, field)| (field.clone(), HashSet::new())).collect();
        TableReferences {
            table,
            position_per_field: HashMap::new(),
            values_per_field,
        }
    }

    fn is_empty(&self) -> bool {
        self.values_per_field.values().all(|x| x.is_empty())
    }

    fn has_referenced_fields(&self) -> bool {
        !self.values_per_field.is_empty()
    }

    fn has_resolved_positions(&self) -> bool {
        self.values_per_field.keys().all(|k| self.position_per_field.contains_key(k))
    }

    fn resolve_positions(&mut self, insert_statement: &str) {
        self.position_per_field = get_field_positions(insert_statement);
        assert!(self.has_resolved_positions());
    }

    fn entries(&self) -> impl Iterator<Item=(String, HashSet<String>)> {
         self.values_per_field.iter().map(|(field, values)| (self.table.to_owned() + "." + field.as_str(), values.clone()))
    }

    fn capture(&mut self, insert_statement: &str) {
        if !insert_statement.starts_with("INSERT") {
            return;
        }
        if !self.has_referenced_fields() {
            return;
        }
        if !self.has_resolved_positions() {
            self.resolve_positions(insert_statement);
        }

        let curr_values = get_values(insert_statement);

        for (field, values) in self.values_per_field.iter_mut() {
            let pos = self.position_per_field[field];
            values.insert(curr_values[pos].to_string());
        }
    }
}

#[derive(Debug)]
pub struct References {
    inner: HashMap<String, TableReferences>,
}

impl References {
    pub fn new<'a, I: IntoIterator<Item = &'a FieldCondition>>(iter: I) -> Self {
        let cascades: HashMap<String, Vec<(String, String)>> = iter
            .into_iter()
            .flat_map(|fc| match &fc.test {
                Tests::Cascade(cond) => Some(cond.get_foreign_key()),
                _ => None,
            })
            .into_grouping_map_by(|(table, _)| table.clone())
            .collect();
        let inner: HashMap<String, TableReferences> = cascades.into_iter().map(|(table, tfs)| (table.to_string(), TableReferences::new(tfs))).collect();

        References { inner }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.values().all(|x| x.is_empty())
    }

    pub fn get_lookup_table(&self) -> HashMap<String, HashSet<String>> {
        self.inner.values().flat_map(|v| v.entries()).collect()
    }

    pub fn capture(&mut self, table: &str, insert_statement: &str) {
        if let Some(rf) = self.inner.get_mut(table) { rf.capture(insert_statement); };
    }
}

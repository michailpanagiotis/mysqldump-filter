use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::{parse_insert_fields, parse_insert_values};

#[derive(Debug)]
pub struct TableReferences {
    table: String,
    values_per_field: HashMap<String, HashSet<String>>,
    position_per_field: HashMap<String, usize>,
}

impl TableReferences {
    fn is_empty(&self) -> bool {
        self.values_per_field.values().all(|x| x.is_empty())
    }

    fn has_referenced_fields(&self) -> bool {
        !self.values_per_field.is_empty()
    }

    fn has_resolved_positions(&self) -> bool {
        self.values_per_field.len() == self.position_per_field.len()
    }

    fn resolve_positions(&mut self, insert_statement: &str) {
        self.position_per_field = HashMap::from_iter(parse_insert_fields(insert_statement).into_iter().filter(|(field, _)| self.values_per_field.contains_key(field)));
        assert!(self.has_resolved_positions());
    }

    fn entries(&self) -> impl Iterator<Item=(String, HashSet<String>)> {
         self.values_per_field.iter().map(|(field, values)| (self.table.to_owned() + "." + field.as_str(), values.clone()))
    }

    pub fn capture(&mut self, insert_statement: &str) {
        if !insert_statement.starts_with("INSERT") {
            return;
        }
        if !self.has_referenced_fields() {
            return;
        }
        if !self.has_resolved_positions() {
            self.resolve_positions(insert_statement);
        }

        let values = parse_insert_values(insert_statement);

        for (field, pos) in self.position_per_field.iter() {
            self.values_per_field.get_mut(field).unwrap().insert(values[*pos].to_string());
        }
    }

    pub fn reset(&mut self) {
        for value in self.values_per_field.values_mut() {
            *value = HashSet::new();
        }
    }
}

impl FromIterator<(String, String)> for TableReferences {
    fn from_iter<T: IntoIterator<Item=(String, String)>>(iter: T) -> Self {
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
}

#[derive(Debug)]
pub struct References {
    pub inner: HashMap<String, TableReferences>,
}

impl References {
    pub fn is_empty(&self) -> bool {
        self.inner.values().all(|x| x.is_empty())
    }

    pub fn get_table_references(&self) -> &HashMap<String, TableReferences> {
        &self.inner
    }

    pub fn get_tables(&self) -> impl Iterator<Item=&String> {
        self.inner.keys()
    }

    pub fn get_lookup_table(&self) -> HashMap<String, HashSet<String>> {
        self.inner.values().flat_map(|v| v.entries()).collect()
    }

    pub fn capture(&mut self, table: &str, insert_statement: &str) {
        if let Some(rf) = self.inner.get_mut(table) { rf.capture(insert_statement); };
    }
}

impl FromIterator<(String, String)> for References {
    fn from_iter<T: IntoIterator<Item=(String, String)>>(items: T) -> Self {
        let grouped = items.into_iter().into_group_map_by(|(table, _)| table.clone());
        let inner: HashMap<String, TableReferences> = grouped.into_iter().map(|(table, tfs)| (table.to_string(), TableReferences::from_iter(tfs))).collect();
        References { inner }
    }
}

impl From<References> for HashMap<String, HashSet<String>> {
    fn from(item: References) -> Self {
        item.inner.values().flat_map(|v| v.entries()).collect()
    }
}

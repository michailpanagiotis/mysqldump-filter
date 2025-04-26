use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::{parse_insert_fields, parse_insert_values};
use crate::filters::{FilterCondition, TableField};

#[derive(Debug)]
#[derive(Default)]
#[derive(Clone)]
pub struct TableReferences {
    table: String,
    values_per_field: HashMap<String, HashSet<String>>,
    position_per_field: HashMap<String, usize>,
}

impl TableReferences {
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

impl FromIterator<TableField> for TableReferences {
    fn from_iter<T: IntoIterator<Item = TableField>>(iter: T) -> Self {
        let fields: Vec<TableField> = iter.into_iter().collect();

        let distinct: Vec<&TableField> = fields.iter().unique_by(|s| &s.table).collect();
        if distinct.len() != 1 {
            panic!("fields have different tables");
        }
        let table = distinct[0].table.to_string();
        let values_per_field = fields.clone().into_iter().map(|table_field| (table_field.field.clone(), HashSet::new())).collect();
        TableReferences {
            table,
            position_per_field: HashMap::new(),
            values_per_field,
        }
    }
}

#[derive(Debug)]
pub struct References {
    pub inner: HashMap<String, TableReferences>
}

impl References {
    pub fn get_references_of_table(&self, key: &str) -> TableReferences {
        self.inner.get(key).cloned().unwrap_or_default()
    }

    pub fn get_table_references(&self) -> &HashMap<String, TableReferences> {
        &self.inner
    }

    pub fn get_tables(&self) -> impl Iterator<Item=&String> {
        self.inner.keys()
    }

    pub fn capture(&mut self, table: &str, insert_statement: &str) {
        if let Some(rf) = self.inner.get_mut(table) { rf.capture(insert_statement); };
    }
}

impl<'a> FromIterator<&'a TableReferences> for References {
    fn from_iter<T: IntoIterator<Item=&'a TableReferences>>(items: T) -> Self {
        let mut grouped: HashMap<String, TableReferences> = HashMap::new();
        for item in items.into_iter() {
            grouped.insert(item.table.clone(), item.clone());
        }
        References {
            inner: grouped,
        }
    }
}

impl FromIterator<TableField> for References {
    fn from_iter<T: IntoIterator<Item=TableField>>(items: T) -> Self {
        let grouped: HashMap<String, Vec<TableField>> = items.into_iter().into_group_map_by(|f| f.table.clone());
        let inner: HashMap<String, TableReferences> = grouped.into_iter().map(|(table, tfs)| (table.to_string(), TableReferences::from_iter(tfs))).collect();
        References { inner }
    }
}

impl<'a> FromIterator<&'a FilterCondition> for References {
    fn from_iter<T: IntoIterator<Item=&'a FilterCondition>>(items: T) -> Self {
        let grouped = items.into_iter().map(|fc| fc.get_referenced_field()).into_group_map_by(|f| f.table.clone());
        References {
            inner: grouped.into_iter().map(|(table, tfs)| (table.to_string(), TableReferences::from_iter(tfs))).collect()
        }
    }
}

impl From<References> for HashMap<String, HashSet<String>> {
    fn from(item: References) -> Self {
        item.inner.values().flat_map(|v| v.entries()).collect()
    }
}

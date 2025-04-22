use std::collections::{HashMap, HashSet};
use crate::sql_statement::{FieldPositions, Statement};
use crate::config::TableFilters;

#[derive(Debug)]
#[derive(Clone)]
pub struct ReferenceTracker {
    table: String,
    referenced_fields: HashSet<String>,
    field_positions: Option<FieldPositions>,
    values_per_field: HashMap<String, HashSet<String>>,
}

impl ReferenceTracker {
    pub fn new(table: &str, referenced_fields: &HashSet<String>) -> Self {
        ReferenceTracker {
            table: table.to_string(),
            referenced_fields: referenced_fields.clone(),
            field_positions: None,
            values_per_field: HashMap::new(),
        }
    }

    pub fn merge<'a, I: Iterator<Item=&'a ReferenceTracker>>(table_refs: I) -> HashMap<String, HashSet<String>> {
        let references: HashMap<String, HashSet<String>> = table_refs.fold(HashMap::new(), |mut acc, curr| {
            acc.extend(curr.to_canonical_entries());
            acc
        });

        references
    }

    pub fn to_canonical_entries(&self) -> impl Iterator<Item=(String, HashSet<String>)> {
        self.values_per_field.iter().map(|(field, value)| (self.table.to_owned() + "." + field, value.clone()))
    }

    pub fn capture(&mut self, statement: &Statement) {
        if !statement.is_insert() {
            return;
        }
        if self.field_positions.is_none() {
            self.field_positions = statement.get_field_positions(&self.referenced_fields);
        }
        if let Some(ref mut pos) = self.field_positions {
            for field in self.referenced_fields.iter() {
                let value = pos.get_value(statement, field);
                match self.values_per_field.get_mut(field) {
                    Some(x) => {
                        x.insert(value.to_string());
                    },
                    None => {
                        self.values_per_field.insert(field.to_string(), HashSet::from([value.to_string()]));
                    }
                }
            }
        }
    }
}


#[derive(Debug)]
pub struct InsertTracker<'a> {
    table: String,
    direct_filters: TableFilters,
    field_names: HashSet<String>,
    reference_filters: TableFilters,
    field_positions: Option<FieldPositions>,
    references: Option<&'a HashMap<String, HashSet<String>>>,
}

impl<'a> InsertTracker<'a> {
    pub fn new(
        table: &str,
        filters: &Option<TableFilters>,
        references: Option<&'a HashMap<String, HashSet<String>>>,
    ) -> Self {
        let mut borrowed = filters.clone();
        let concrete_filters = borrowed.get_or_insert(TableFilters::empty());

        InsertTracker {
            table: table.to_string(),
            field_names: concrete_filters.get_filtered_fields().clone(),
            direct_filters: concrete_filters.to_direct_filters(),
            reference_filters: concrete_filters.to_reference_filters(),
            field_positions: None,
            references,
        }
    }

    pub fn should_keep_statement(&mut self, statement: &Statement) -> bool {
        if !statement.is_insert() || statement.get_table().is_none_or(|ref t| t != &self.table) {
            return true;
        }

        if self.field_positions.is_none() {
            self.field_positions = statement.get_field_positions(&self.field_names);
        }

        let Some(ref pos) = self.field_positions else { return true };

        let value_per_field = pos.get_values(statement, &self.field_names);
        if !self.direct_filters.test(&value_per_field) {
            return false;
        }

        let Some(ref refs) = self.references else { return true };

        dbg!("HERE");
        if !self.reference_filters.test(&value_per_field) {
            return false;
        }

        true
    }
}

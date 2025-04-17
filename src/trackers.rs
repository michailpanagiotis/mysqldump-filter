use std::collections::{HashMap, HashSet};
use crate::sql_statement::{FieldPositions, Statement};
use crate::config::{FilterMap, TableFilters};

#[derive(Debug)]
pub struct ReferenceTracker {
    references: HashMap<String, HashSet<String>>,
    is_complete: bool,
}

impl ReferenceTracker {
    pub fn new() -> Self {
        ReferenceTracker {
            references: HashMap::new(),
            is_complete: false,
        }
    }

    pub fn from_iter<'a, I: Iterator<Item=&'a ReferenceTracker>>(ref_trackers: I) -> Self {
        let mut references = HashMap::new();

        for tracker in ref_trackers {
            for (key, value) in &tracker.references {
                references.insert(key.to_string(), value.clone());
            }
        }

        ReferenceTracker {
            references,
            is_complete: true,
        }
    }

    pub fn get_key(&mut self, table: &String, field: &str) -> String {
        table.to_owned() + "." + field
    }

    pub fn has_completed(&self) -> bool {
        self.is_complete
    }


    pub fn insert(&mut self, table: &String, field: &str, value: &String) {
        let key: String = self.get_key(table, field);
        match self.references.get_mut(&key) {
            Some(x) => {
                x.insert(value.to_string());
            },
            None => {
                self.references.insert(key, HashSet::from([value.to_string()]));
            }
        }
    }
}

#[derive(Debug)]
pub struct InsertTracker {
    direct_filters: TableFilters,
    reference_filters: TableFilters,
    references: HashSet<String>,
    field_positions: FieldPositions,
    reference_tracker: ReferenceTracker,
}

impl InsertTracker {
    pub fn new(
        table: &String,
        filters_per_table: &FilterMap,
        references_per_table: &HashMap<String, Vec<String>>,
        field_positions: &FieldPositions,
    ) -> Self {
        let filters = filters_per_table.get(table);
        let references = match references_per_table.get(table) {
            Some(x) => x.clone(),
            None => Vec::new(),
        };
        InsertTracker {
            direct_filters: filters.to_direct_filters(),
            reference_filters: filters.to_reference_filters(),
            references: HashSet::from_iter(references.iter().cloned()),
            field_positions: field_positions.clone(),
            reference_tracker: ReferenceTracker::new(),
        }
    }

    pub fn capture_references(&mut self, statement: &Statement) {
        if let Some(ref table) = statement.get_table() {
            for field in self.references.iter() {
                let value = self.field_positions.get_value(statement, field);
                self.reference_tracker.insert(table, field, &value);
            }
        }
    }

    pub fn should_keep_statement(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) -> bool {
        let value_per_field = self.field_positions.get_values(
            statement,
            self.direct_filters.get_filtered_fields(),
        );

        if !self.direct_filters.test(&value_per_field) {
            return false;
        }

        if reference_tracker.has_completed() && !self.reference_filters.test(&value_per_field) {
            return false;
        }

        true
    }

    pub fn get_reference_tracker(&self) -> &ReferenceTracker {
        &self.reference_tracker
    }
}

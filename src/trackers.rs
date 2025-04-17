use std::collections::{HashMap, HashSet};
use crate::sql_statement::{FieldPositions, Statement};
use crate::config::{FilterMap, TableFilters};

#[derive(Debug)]
#[derive(Clone)]
pub struct TableReferences {
    table: String,
    referenced_fields: HashSet<String>,
    field_positions: Option<FieldPositions>,
    values_per_field: HashMap<String, HashSet<String>>,
    is_complete: bool,
}

impl TableReferences {
    pub fn new(table: &String, referenced_fields: &HashSet<String>) -> Self {
        TableReferences {
            table: table.clone(),
            referenced_fields: referenced_fields.clone(),
            field_positions: None,
            values_per_field: HashMap::new(),
            is_complete: false,
        }
    }

    fn get_table(&self) -> String {
        self.table.clone()
    }

    pub fn has_completed(&self) -> bool {
        self.is_complete
    }

    pub fn insert(&mut self, field: &str, value: &String) {
        match self.values_per_field.get_mut(field) {
            Some(x) => {
                x.insert(value.to_string());
            },
            None => {
                self.values_per_field.insert(field.to_string(), HashSet::from([value.to_string()]));
            }
        }
    }

    pub fn to_canonical_entries(&self) -> impl Iterator<Item=(String, HashSet<String>)> {
        self.values_per_field.iter().map(|(field, value)| (self.table.to_owned() + "." + field, value.clone()))
    }

    pub fn capture(&mut self, statement: &Statement) {
        if self.field_positions.is_none() {
            self.field_positions = statement.get_filtered_field_positions(&self.referenced_fields);
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
pub struct ReferenceTracker {
    referenced_fields: HashSet<String>,
    table_references: HashMap<String, TableReferences>,
    is_complete: bool,
}

impl ReferenceTracker {
    pub fn new(referenced_fields: &HashSet<String>) -> Self {
        ReferenceTracker {
            referenced_fields: referenced_fields.clone(),
            table_references: HashMap::new(),
            is_complete: false,
        }
    }

    pub fn from_iter<'a, I: Iterator<Item=&'a TableReferences>>(table_refs: I) -> Self {
        let mut references: HashMap<String, HashSet<String>> = HashMap::new();
        let mut table_references: HashMap<String, TableReferences> = HashMap::new();

        for tref in table_refs {
            for (field, value) in tref.to_canonical_entries() {
                references.insert(field, value);
            }
            table_references.insert(tref.get_table(), tref.clone());
        }

        ReferenceTracker {
            referenced_fields: HashSet::new(),
            table_references,
            is_complete: true,
        }
    }

    pub fn has_completed(&self) -> bool {
        self.is_complete
    }


    pub fn insert(&mut self, table: &String, field: &str, value: &String) {
        match self.table_references.get_mut(table) {
            Some(x) => {
                x.insert(field, value);
            },
            None => {
                let mut refs = TableReferences::new(table, &self.referenced_fields);
                refs.insert(field, value);
                self.table_references.insert(table.clone(), refs);
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
        let referenced_fields = match references_per_table.get(table) {
            Some(x) => HashSet::from_iter(x.iter().cloned()),
            None => HashSet::new(),
        };

        InsertTracker {
            direct_filters: filters.to_direct_filters(),
            reference_filters: filters.to_reference_filters(),
            references: HashSet::from_iter(references.iter().cloned()),
            field_positions: field_positions.clone(),
            reference_tracker: ReferenceTracker::new(&referenced_fields),
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

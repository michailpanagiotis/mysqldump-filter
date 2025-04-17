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
}

impl TableReferences {
    pub fn new(table: &str, referenced_fields: &HashSet<String>) -> Self {
        TableReferences {
            table: table.to_string(),
            referenced_fields: referenced_fields.clone(),
            field_positions: None,
            values_per_field: HashMap::new(),
        }
    }

    fn get_table(&self) -> String {
        self.table.clone()
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
}

#[derive(Debug)]
pub struct InsertTracker {
    direct_filters: TableFilters,
    reference_filters: TableFilters,
    field_positions: FieldPositions,
}

impl InsertTracker {
    pub fn new(
        table: &str,
        filters_per_table: &FilterMap,
        field_positions: &FieldPositions,
    ) -> Self {
        let filters = filters_per_table.get(table);

        InsertTracker {
            direct_filters: filters.to_direct_filters(),
            reference_filters: filters.to_reference_filters(),
            field_positions: field_positions.clone(),
        }
    }

    pub fn should_keep_statement(&mut self, statement: &Statement) -> bool {
        let value_per_field = self.field_positions.get_values(
            statement,
            self.direct_filters.get_filtered_fields(),
        );

        if !self.direct_filters.test(&value_per_field) {
            return false;
        }

        true
    }
}

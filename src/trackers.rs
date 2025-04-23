use std::collections::{HashMap, HashSet};

use crate::sql_statement::Statement;
use crate::filters::{TableField, TableReferences};

#[derive(Debug)]
#[derive(Clone)]
pub struct ReferenceTracker {
    table_refs: TableReferences,
}

impl ReferenceTracker {
    pub fn new(table: &str, referenced_fields: &HashSet<String>) -> Self {
        let table_refs = TableReferences::from_iter(
            referenced_fields.iter().map(|field| TableField {
                table: table.to_string(),
                field: field.clone(),
            })
        );
        ReferenceTracker {
            table_refs,
        }
    }

    pub fn merge<I: Iterator<Item=ReferenceTracker>>(table_refs: I) -> HashMap<String, HashSet<String>> {
        let references: HashMap<String, HashSet<String>> = table_refs.fold(HashMap::new(), |mut acc, tracker| {
            let rfs = HashMap::from(tracker.table_refs);
            acc.extend(rfs);
            acc
        });

        references
    }

    pub fn capture(&mut self, statement: &Statement) {
        if !statement.is_insert() {
            return;
        }
        self.table_refs.capture(statement.as_str());
    }
}

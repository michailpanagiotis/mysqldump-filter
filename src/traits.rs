use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use thiserror::Error;
use std::rc::Weak;
use std::cell::RefCell;

use crate::column::ColumnMeta;
use crate::sql::get_column_positions;

pub trait ColumnPositions {
    fn get_column_positions(&self) -> &Option<HashMap<String, usize>>;

    fn set_column_positions(&mut self, positions: HashMap<String, usize>);

    fn resolve_column_positions(&mut self, insert_statement: &str) {
        if !self.has_resolved_positions() {
            self.set_column_positions(get_column_positions(insert_statement));
            assert!(self.has_resolved_positions());
        }
    }

    fn has_resolved_positions(&self) -> bool {
        self.get_column_positions().is_some()
    }

    fn pick_values<'a, 'b, I: Iterator<Item=&'b ColumnMeta>>(&self, columns: I, values: &'a [&'a str]) -> HashMap<String, &'a str> {
        let Some(positions) = self.get_column_positions() else { return HashMap::new() };
        columns.map(|c| (c.get_column_key().to_owned(), values[positions[c.get_column_name()]])).collect()
    }
}

pub trait ReferenceTracker: ColumnPositions {
    fn get_referenced_columns(&self) -> impl Iterator<Item=&ColumnMeta>;
    fn get_references(&self) -> &HashMap<String, HashSet<String>>;
    fn get_references_mut(&mut self) -> &mut HashMap<String, HashSet<String>>;

    fn capture_references(&mut self, values: &[&str]) -> Result<(), anyhow::Error> {
        let to_insert = self.pick_values(self.get_referenced_columns(), values);
        let references = self.get_references_mut();
        for (key, value) in to_insert.into_iter() {
            let Some(r) = references.get_mut(&key) else { return Err(anyhow::anyhow!("No references set for '{}'", key)) };
            r.insert(value.to_owned());
        }
        Ok(())
    }
}

pub trait Dependency {
    fn set_fulfilled_at_depth(&mut self, depth: &usize);
    fn has_been_fulfilled(&self) -> bool;

    fn get_dependencies(&self) -> &[Weak<RefCell<dyn Dependency>>];

    fn has_fulfilled_dependencies(&self) -> bool {
        self.get_dependencies().iter().all(|d| {
            d.upgrade().unwrap().borrow().has_been_fulfilled()
        })
    }

    fn fulfill_dependency(&mut self, depth: &usize) {
        if !self.has_been_fulfilled() {
            self.set_fulfilled_at_depth(depth);
        }
        assert!(self.has_been_fulfilled());
    }
}

#[derive(Debug)]
#[derive(Error)]
pub struct NoDataTypeError;

impl std::fmt::Display for NoDataTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "no data type")
    }
}

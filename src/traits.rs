use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use thiserror::Error;

use crate::sql::get_column_positions;
use std::cell::RefCell;
use std::rc::{Rc, Weak};

pub trait DBColumn {
    fn get_column_meta(&self) -> &ColumnMeta;

    fn get_table_name(&self) -> &str {
        &self.get_column_meta().table
    }

    fn get_column_name(&self) -> &str {
        &self.get_column_meta().column
    }

    fn get_column_key(&self) -> &str {
        &self.get_column_meta().key
    }

    fn get_data_type(&self) -> &sqlparser::ast::DataType {
        &self.get_column_meta().data_type
    }
}

pub trait ColumnPositions {
    fn get_column_positions(&self) -> &Option<HashMap<String, usize>>;

    fn set_column_positions(&mut self, positions: HashMap<String, usize>);

    fn resolve_column_positions(&mut self, insert_statement: &str) {
        if !self.has_resolved_positions() {
            self.set_column_positions(get_column_positions(insert_statement));
            assert!(self.has_resolved_positions());
        }
    }

    fn get_column_position(&self, column_name: &str) -> Option<usize> {
        let positions = self.get_column_positions().as_ref()?;
        Some(positions[column_name])
    }

    fn has_resolved_positions(&self) -> bool {
        self.get_column_positions().is_some()
    }

    fn pick_values<'a>(&self, columns: &HashSet<ColumnMeta>, values: &'a [&'a str]) -> HashMap<String, &'a str> {
        let Some(positions) = self.get_column_positions() else { return HashMap::new() };
        columns.iter().map(|c| (c.key.to_owned(), values[positions[&c.column]])).collect()
    }
}

pub trait ReferenceTracker: ColumnPositions {
    fn get_referenced_columns(&self) -> &HashSet<ColumnMeta>;
    fn get_referenced_columns_mut(&mut self) -> &mut HashSet<ColumnMeta>;
    fn get_references(&self) -> &HashMap<String, HashSet<String>>;
    fn get_references_mut(&mut self) -> &mut HashMap<String, HashSet<String>>;

    fn add_referenced_column(&mut self, dep: &ColumnMeta) {
        self.get_referenced_columns_mut().insert(dep.to_owned());
        self.get_references_mut().insert(dep.key.to_owned(), HashSet::new());
    }

    fn capture_references(&mut self, values: &[&str]) {
        let to_insert = self.pick_values(self.get_referenced_columns(), values);
        let references = self.get_references_mut();
        for (key, value) in to_insert.into_iter() {
            references.get_mut(&key).unwrap().insert(value.to_owned());
        }
    }
}

pub trait Dependency {
    fn set_fulfilled_at_depth(&mut self, depth: &usize);
    fn has_been_fulfilled(&self) -> bool;
    fn get_dependencies(&self) -> &Vec<Weak<RefCell<dyn Dependency>>>;
    fn get_dependencies_mut(&mut self) -> &mut Vec<Weak<RefCell<dyn Dependency>>>;

    fn add_dependency(&mut self, target: Weak<RefCell<dyn Dependency>>) {
        self.get_dependencies_mut().push(target);
    }

    fn is_ready_to_be_tested(&self) -> bool {
        !self.has_been_fulfilled() && self.get_dependencies().iter().all(|d| {
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

pub trait ColumnTest: DBColumn {
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, anyhow::Error> where Self: Sized;

    fn test(&self, value:&str, lookup_table: &HashMap<String, HashSet<String>>) -> bool;

    fn get_column_dependencies(&self) -> HashSet<ColumnMeta> {
        HashSet::new()
    }

    fn get_tracked_columns(&self) -> HashSet<ColumnMeta> {
        let mut res: HashSet<ColumnMeta> = HashSet::from([self.get_column_meta().to_owned()]);
        for dep in self.get_column_dependencies() {
            res.insert(dep.to_owned());
        }
        res
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

#[derive(Clone)]
#[derive(Debug)]
#[derive(Hash)]
#[derive(Eq, PartialEq)]
pub struct ColumnMeta {
    pub key: String,
    pub table: String,
    pub column: String,
    data_type: sqlparser::ast::DataType,
}

impl DBColumn for ColumnMeta {
    fn get_column_meta(&self) -> &ColumnMeta {
        self
    }
}

impl ColumnMeta {
    pub fn new(table: &str, column: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<Self, anyhow::Error> {
        let key = table.to_owned() + "." + column;
        let Some(data_type) = data_types.get(&key) else { return Err(anyhow::anyhow!("No data type: {}", key)) };
        Ok(Self {
            key,
            table: table.to_owned(),
            column: column.to_string(),
            data_type: data_type.to_owned(),
        })
    }
}

impl core::fmt::Debug for dyn ColumnTest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.get_column_meta().fmt(f)
    }
}

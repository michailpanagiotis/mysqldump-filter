use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use thiserror::Error;

use crate::sql::get_column_positions;
use std::cell::RefCell;
use std::rc::Weak;


pub trait DBColumn {
    fn get_column_meta(&self) -> &ColumnMeta;
    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta;

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

    fn has_resolved_positions(&self) -> bool {
        self.get_column_positions().is_some()
    }

    fn pick_values<'a, 'b, I: Iterator<Item=&'b ColumnMeta>>(&self, columns: I, values: &'a [&'a str]) -> HashMap<String, &'a str> {
        let Some(positions) = self.get_column_positions() else { return HashMap::new() };
        columns.map(|c| (c.key.to_owned(), values[positions[&c.column]])).collect()
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
    fn get_dependencies(&self) -> &Vec<Weak<RefCell<dyn Dependency>>>;

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

pub trait ColumnTest: DBColumn {
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, anyhow::Error> where Self: Sized;

    fn test(&self, value:&str, lookup_table: &HashMap<String, HashSet<String>>) -> bool;

    fn get_definition(&self) -> &str;
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
#[derive(PartialEq)]
pub struct ColumnMeta {
    key: String,
    table: String,
    column: String,
    data_type: sqlparser::ast::DataType,
    is_referenced: bool,
    dependency_keys: Vec<String>,
    checks: Vec<String>,
    dependencies: Vec<ColumnMeta>,
    position: Option<usize>,
}

impl DBColumn for ColumnMeta {
    fn get_column_meta(&self) -> &ColumnMeta {
        self
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        self
    }
}

impl ColumnMeta {
    pub fn get_components_from_key(key: &str) -> Result<(String, String), anyhow::Error> {
        let mut split = key.split('.');
        let (Some(table), Some(column), None) = (split.next(), split.next(), split.next()) else {
            return Err(anyhow::anyhow!("malformed key {}", key));
        };
        Ok((table.to_owned(), column.to_owned()))
    }

    fn get_key_from_components(table: &str, column: &str) -> String {
        table.to_owned() + "." + column
    }

    pub fn new(table: &str, column: &str, dependency_keys: &[&str], data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<Self, anyhow::Error> {
        let key = table.to_owned() + "." + column;
        let Some(data_type) = data_types.get(&key) else { return Err(anyhow::anyhow!("No data type: {}", key)) };
        Ok(Self {
            key,
            table: table.to_owned(),
            column: column.to_string(),
            data_type: data_type.to_owned(),
            is_referenced: false,
            dependency_keys: dependency_keys.iter().map(|x| x.to_string()).collect(),
            checks: Vec::new(),
            dependencies: Vec::new(),
            position: None,
        })
    }

    pub fn capture_position(&mut self, positions: &HashMap<String, usize>) {
        self.position = Some(positions[self.get_column_name()]);
    }

    pub fn get_column_dependencies(&self) -> impl Iterator<Item=&ColumnMeta> {
        self.dependencies.iter()
    }

    pub fn get_referenced_columns(&self) -> impl Iterator<Item=&ColumnMeta> {
        std::iter::once(self).chain(self.dependencies.iter())
    }

    pub fn get_checks(&self) -> impl Iterator<Item=&String> {
        self.checks.iter()
    }

    pub fn add_check(&mut self, check_definition: &str) {
        self.checks.push(check_definition.to_owned());
    }

    pub fn get_dependency_keys(&self) -> impl Iterator<Item=&String> {
        self.dependency_keys.iter()
    }

    pub fn add_dependency_key(&mut self, dependency_key: &str) {
        self.dependency_keys.push(dependency_key.to_owned());
    }

    pub fn is_referenced(&self) -> bool {
        self.is_referenced
    }

    pub fn set_referenced(&mut self) {
        self.is_referenced = true
    }

    pub fn extend(&mut self, other: &ColumnMeta) {
        if self.is_referenced() || other.is_referenced() {
            self.set_referenced();
        }
        for check in other.get_checks() {
            self.add_check(check)
        }
        for key in other.get_dependency_keys() {
            self.add_dependency_key(key)
        }
    }
}

impl core::fmt::Debug for dyn ColumnTest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.get_column_meta().fmt(f)
    }
}

impl Extend<ColumnMeta> for HashMap<std::string::String, ColumnMeta> {
    fn extend<T: IntoIterator<Item=ColumnMeta>>(&mut self, iter: T) {
        for elem in iter {
            let key = elem.get_column_name();
            match self.get_mut(key) {
                None => {
                    self.insert(key.to_owned(), elem);
                },
                Some(cm) => {
                    cm.extend(&elem);
                }
            }
        }
    }
}

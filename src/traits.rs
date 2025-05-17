use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use crate::sql::get_column_positions;

pub trait DBColumn {
    fn get_column_meta(&self) -> &ColumnMeta;

    fn get_column_name(&self) -> &str {
        &self.get_column_meta().column
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
}

pub trait ColumnTest: DBColumn {
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> impl ColumnTest + 'static where Self: Sized;

    fn test(&self, value:&str, lookup_table: &HashMap<String, HashSet<String>>) -> bool;

    fn get_dependencies(&self) -> HashSet<ColumnMeta> {
        HashSet::new()
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
    pub fn new(table: &str, column: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        let key = table.to_owned() + "." + column;
        let data_type = match data_types.get(&key) {
            None => panic!("{}", format!("cannot find data type for {key}")),
            Some(data_type) => data_type.to_owned()
        };
        Self {
            key,
            table: table.to_owned(),
            column: column.to_string(),
            data_type,
        }
    }
}

impl core::fmt::Debug for dyn ColumnTest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.get_column_meta().fmt(f)
    }
}

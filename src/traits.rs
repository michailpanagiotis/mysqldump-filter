use std::collections::{HashMap, HashSet};
use std::fmt::Debug;


pub trait DBColumn {
    fn get_column_meta(&self) -> &ColumnMeta;
    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta;

    fn get_column_name(&self) -> &str {
        &self.get_column_meta().column
    }

    fn get_data_type(&self) -> &sqlparser::ast::DataType {
        &self.get_column_meta().data_type
    }

    fn get_column_position(&self) -> &Option<usize> {
        &self.get_column_meta().position
    }

    fn has_resolved_position(&self) -> bool {
        self.get_column_meta().has_resolved_position()
    }

    fn set_position(&mut self, pos: usize) {
        self.get_column_meta_mut().set_position(pos);
    }

    fn set_position_from_column_positions(&mut self, positions: &HashMap<String, usize>) {
        match positions.get(self.get_column_name()) {
            Some(pos) => self.set_position(*pos),
            None => panic!("{}", format!("unknown column {}", self.get_column_name())),
        }
    }
}

pub trait TestValue: DBColumn {
    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool;

    fn get_dependencies(&self) -> HashSet<ColumnMeta> {
        HashSet::new()
    }

    fn test_row(&self, values: &[&str], lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        self.get_column_position().is_some_and(|p| self.test(values[p], lookup_table))
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
            position: None,
        }
    }

    fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }

    fn has_resolved_position(&self) -> bool {
        self.position.is_some()
    }
}

impl core::fmt::Debug for dyn TestValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.get_column_meta().fmt(f)
    }
}

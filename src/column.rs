use std::collections::HashMap;
use std::fmt::Debug;

pub trait DBColumn {
    fn get_column_name(&self) -> &str;
    fn get_data_type(&self) -> &sqlparser::ast::DataType;
}

#[derive(Clone)]
#[derive(Debug)]
#[derive(PartialEq)]
pub struct ColumnMeta {
    key: String,
    table: String,
    column: String,
    data_type: sqlparser::ast::DataType,
    foreign_keys: Vec<String>,
    position: Option<usize>,
    is_referenced: bool,
    checks: Vec<String>,
    tested_at_pass: Option<usize>,
}

impl DBColumn for ColumnMeta {
    fn get_column_name(&self) -> &str {
        &self.column
    }

    fn get_data_type(&self) -> &sqlparser::ast::DataType {
        &self.data_type
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

    pub fn new(
        table: &str,
        column: &str,
        foreign_keys: &[String],
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<Self, anyhow::Error> {
        let key = ColumnMeta::get_key_from_components(table, column);
        let Some(data_type) = data_types.get(&key) else { return Err(anyhow::anyhow!("No data type: {}", key)) };
        Ok(Self {
            key,
            table: table.to_owned(),
            column: column.to_string(),
            data_type: data_type.to_owned(),
            is_referenced: false,
            foreign_keys: foreign_keys.iter().map(|x| x.to_string()).collect(),
            checks: Vec::new(),
            position: None,
            tested_at_pass: None,
        })
    }

    pub fn from_foreign_key(key: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<Self, anyhow::Error> {
        let (target_table, target_column) = ColumnMeta::get_components_from_key(key)?;
        let mut target_column_meta = ColumnMeta::new(&target_table, &target_column, &Vec::new(), data_types)?;
        target_column_meta.set_referenced();
        Ok(target_column_meta)
    }

    pub fn get_table_name(&self) -> &str {
        &self.table
    }

    pub fn get_column_name(&self) -> &str {
        &self.column
    }

    pub fn get_column_key(&self) -> &str {
        &self.key
    }

    pub fn get_data_type(&self) -> &sqlparser::ast::DataType {
        &self.data_type
    }

    pub fn capture_position(&mut self, positions: &HashMap<String, usize>) {
        self.position = Some(positions[self.get_column_name()]);
    }

    pub fn get_checks(&self) -> impl Iterator<Item=&String> {
        self.checks.iter()
    }

    pub fn add_check(&mut self, check_definition: &str) {
        self.checks.push(check_definition.to_owned());
    }

    pub fn get_foreign_keys(&self) -> impl Iterator<Item=&String> {
        self.foreign_keys.iter()
    }

    pub fn get_foreign_tables(&self) -> Result<Vec<String>, anyhow::Error> {
        let mut tables: Vec<String> = Vec::new();
        for key in self.foreign_keys.iter() {
            let (table, _) = ColumnMeta::get_components_from_key(key)?;
            tables.push(table.to_owned());
        }
        Ok(tables)
    }

    pub fn add_foreign_key(&mut self, dependency_key: &str) {
        self.foreign_keys.push(dependency_key.to_owned());
    }

    pub fn is_referenced(&self) -> bool {
        self.is_referenced
    }

    pub fn set_referenced(&mut self) {
        self.is_referenced = true
    }

    pub fn set_fulfilled_at_depth(&mut self, depth: &usize) {
        self.tested_at_pass = Some(depth.to_owned());
    }

    pub fn has_been_fulfilled(&self) -> bool {
        self.tested_at_pass.is_some()
    }

    pub fn extend(&mut self, other: &ColumnMeta) {
        if self.is_referenced() || other.is_referenced() {
            self.set_referenced();
        }
        for check in other.get_checks() {
            self.add_check(check)
        }
        for key in other.get_foreign_keys() {
            self.add_foreign_key(key)
        }
    }
}

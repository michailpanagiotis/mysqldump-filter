mod dependencies;

use cel_interpreter::{Context, Program};
use chrono::NaiveDateTime;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::checks::dependencies::{DependencyNode, chunk_by_depth};

pub type PlainCheckType = Box<dyn PlainColumnCheck>;

enum Value {
    Int(i64),
    Date(i64),
    String(String),
    Null
}

impl Value {
    fn parse_int(s: &str) -> i64 {
        s.parse().unwrap_or_else(|_| panic!("cannot parse int {s}"))
    }

    fn parse_string(s: &str) -> String {
        s.replace("'", "")
    }

    fn parse_date(s: &str) -> i64 {
        let date = Value::parse_string(s);
        let to_parse = if date.len() == 10 { date.to_owned() + " 00:00:00" } else { date.to_owned() };
        if to_parse.starts_with("0000-00-00") {
            return NaiveDateTime::MIN.and_utc().timestamp();
        }
        NaiveDateTime::parse_from_str(&to_parse, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| panic!("cannot parse timestamp {s}"))
            .and_utc()
            .timestamp()
    }

    fn parse(value: &str, data_type: &sqlparser::ast::DataType) -> Self {
        if value == "NULL" {
            return Value::Null;
        }
        match data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                Value::Int(Value::parse_int(value))
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                Value::Date(Value::parse_date(value))
            },
            _ => Value::String(Value::parse_string(value))
        }
    }
}

/// Trait for column-based filtering and validation logic
///
/// Implementors define specific types of checks that can be applied to column values
/// within SQL INSERT statements, such as CEL expression evaluation or foreign key lookups.
pub trait PlainColumnCheck {
    fn new(definition: &str, table: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized;

    fn test_value(
        &self,
        value: &str,
        data_type: &sqlparser::ast::DataType,
        lookup_table: &mut HashMap<String, HashSet<String>>,
    ) -> Result<bool, anyhow::Error>;

    fn get_table_name(&self) -> &str;

    fn get_column_name(&self) -> &str;

    fn get_column_key(&self) -> &str;

    fn get_definition(&self) -> &str;

    fn get_key(&self) -> &str;

    fn get_tracked_columns(&self) -> Vec<&str>;

    fn as_any(&self) -> &dyn Any;
}

impl<'a> From<&'a PlainCheckType> for String {
    fn from(item: &'a PlainCheckType) -> Self {
        item.get_key().to_owned()
    }
}

#[derive(Debug)]
struct CheckDefinition (String, String);

impl<'a> From<&'a CheckDefinition> for String {
    fn from(item: &'a CheckDefinition) -> Self {
        // dbg!(&item);
        if !item.1.contains("->") {
            return item.0.to_string() + "." + item.1.as_str();
        }

        let definition = item.0.as_str().to_owned() + "." + item.1.as_str();
        let column = definition.split("->").next().unwrap();
        column.to_owned()
    }
}

impl core::fmt::Debug for dyn PlainColumnCheck {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        (self.get_key()).fmt(f)
    }
}

/// CEL (Common Expression Language) based column filter
///
/// Evaluates CEL expressions against column values to determine if records
/// should be included in the filtered output.
#[derive(Debug)]
pub struct PlainCelTest {
    key: String,
    table_name: String,
    column_name: String,
    column_key: String,
    definition: String,
    program: Program,
}

impl PlainCelTest {
    pub fn get_column_info(definition: &str) -> Result<(String, Vec<String>), anyhow::Error> {
        let program = Program::compile(definition)?;
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        if variables.len() > 1 {
            return Err(anyhow::anyhow!("Each CEL test must have only one variable"));
        }
        let column_name = &variables[0];
        Ok((column_name.to_owned(), Vec::new()))
    }

    fn parse_date(s: &str) -> i64 {
        let to_parse = if s.len() == 10 { s.to_owned() + " 00:00:00" } else { s.to_owned() };
        NaiveDateTime::parse_from_str(&to_parse, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| panic!("cannot parse timestamp {s}"))
            .and_utc()
            .timestamp()
    }

    fn build_context(&self, column_name: &str, str_value: &str, data_type: &sqlparser::ast::DataType) -> Result<Context, anyhow::Error> {
        let value: Value = Value::parse(str_value, data_type);
        let mut context = Context::default();
        context.add_function("timestamp", |d: Arc<String>| {
            PlainCelTest::parse_date(&d)
        });

        let e = anyhow::anyhow!("Cannot add variable to context");
        match value {
            Value::Int(parsed) => context.add_variable(column_name, parsed),
            Value::Date(parsed) => context.add_variable(column_name, parsed),
            Value::String(parsed) => context.add_variable(column_name, parsed),
            Value::Null => context.add_variable(column_name, false),
        }.map_err(|_| e)?;

        Ok(context)
    }
}

impl PlainColumnCheck for PlainCelTest {
    fn new(definition: &str, table: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];

        Ok(PlainCelTest {
            key: String::from("cel: ") + table + ": " + definition,
            table_name: table.to_owned(),
            column_name: column.to_owned(),
            column_key: String::from(table) + "." +column,
            definition: definition.to_owned(),
            program,
        })
    }

    fn test_value(
        &self,
        value: &str,
        data_type: &sqlparser::ast::DataType,
        _lookup_table: &mut HashMap<String, HashSet<String>>,
    ) -> Result<bool, anyhow::Error> {
        let context = self.build_context(self.get_column_name(), value, data_type)?;
        match self.program.execute(&context)? {
            cel_interpreter::objects::Value::Bool(v) => {
                // println!("testing {}.{} {} -> {}", self.table, self.column, &other_value, &v);
                Ok(v)
            }
            _ => panic!("filter does not return a boolean"),
        }
    }

    fn get_key(&self) -> &str {
        &self.key
    }

    fn get_definition(&self) -> &str {
        &self.definition
    }

    fn get_table_name(&self) -> &str {
        &self.table_name
    }

    fn get_column_name(&self) -> &str {
        &self.column_name
    }

    fn get_column_key(&self) -> &str {
        &self.column_key
    }

    fn get_tracked_columns(&self) -> Vec<&str> {
        Vec::new()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Foreign key lookup-based filter
///
/// Tests whether a column value exists in a set of previously collected values
/// from another table, implementing cascading inclusion based on relationships.
#[derive(Debug)]
pub struct PlainLookupTest {
    key: String,
    table_name: String,
    column_name: String,
    column_key: String,
    definition: String,
    target_column_key: String,
}

impl PlainLookupTest {
    pub fn get_column_info(definition: &str) -> Result<(String, Vec<String>), anyhow::Error> {
        let mut split = definition.split("->");
        let (Some(column_name), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };
        Ok((column_name.to_owned(), Vec::from([foreign_key.to_owned()])))
    }
}

impl PlainColumnCheck for PlainLookupTest {
    fn new(definition: &str, table: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized {
        let mut split = definition.split("->");
        let (Some(source_column), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };

        Ok(PlainLookupTest {
            key: String::from("lookup: ") + table + ": " + definition,
            table_name: table.to_owned(),
            column_name: source_column.to_owned(),
            column_key: String::from(table) + "." + source_column,
            definition: definition.to_owned(),
            target_column_key: foreign_key.to_owned(),
        })
    }

    fn test_value(
        &self,
        value: &str,
        _data_type: &sqlparser::ast::DataType,
        lookup_table: &mut HashMap<String, HashSet<String>>,
    ) -> Result<bool, anyhow::Error> {
        let Some(set) = lookup_table.get(&self.target_column_key) else { return Ok(true) };
        Ok(set.contains(value))
    }

    fn get_key(&self) -> &str {
        &self.key
    }

    fn get_definition(&self) -> &str {
        &self.definition
    }

    fn get_table_name(&self) -> &str {
        &self.table_name
    }

    fn get_column_name(&self) -> &str {
        &self.column_name
    }

    fn get_column_key(&self) -> &str {
        &self.column_key
    }

    fn get_tracked_columns(&self) -> Vec<&str> {
        Vec::new()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Value tracking filter for building lookup tables
///
/// Collects values from specified columns to build lookup tables that can be
/// used by other filters for cascading inclusion logic.
#[derive(Debug)]
pub struct PlainTrackingTest {
    key: String,
    table_name: String,
    column_name: String,
    column_key: String,
    definition: String,
}

impl PlainColumnCheck for PlainTrackingTest {
    fn new(definition: &str, table_name: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized {
        let mut split = definition.split(".");
        let (Some(table), Some(column), None) = (split.next(), split.next(), split.next()) else {
            return Err(anyhow::anyhow!("cannot parse test"));
        };

        if table != table_name {
            return Err(anyhow::anyhow!("table name mismatch"));
        }

        Ok(PlainTrackingTest {
            key: String::from("track: ") + table + ": " + definition,
            table_name: table.to_owned(),
            column_name: column.to_owned(),
            column_key: String::from(table) + "." + column,
            definition: definition.to_owned(),
        })
    }

    fn test_value(
        &self,
        value: &str,
        _data_type: &sqlparser::ast::DataType,
        lookup_table: &mut HashMap<String, HashSet<String>>,
    ) -> Result<bool, anyhow::Error> {
        let key = self.get_column_key();
        match lookup_table.get_mut(key) {
            None => { lookup_table.insert(self.get_column_key().to_owned(), HashSet::from([value.to_owned()])); }
            Some(values) => { values.insert(value.to_owned()); }
        }
        Ok(true)
    }

    fn get_key(&self) -> &str {
        &self.key
    }

    fn get_definition(&self) -> &str {
        &self.definition
    }

    fn get_table_name(&self) -> &str {
        &self.table_name
    }

    fn get_column_name(&self) -> &str {
        &self.column_name
    }

    fn get_column_key(&self) -> &str {
        &self.column_key
    }

    fn get_tracked_columns(&self) -> Vec<&str> {
        Vec::from([self.get_column_key()])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct TableChecks { checks: Vec<PlainCheckType>, text_transforms: HashMap<String, String> }

impl TableChecks {
    pub fn new(mut checks: Vec<PlainCheckType>, text_transforms: Option<&HashMap<String, String>>) -> Self {
        // tests have implicit order
        checks.sort_by_key(|a| {
            if a.as_any().downcast_ref::<PlainTrackingTest>().is_some() {
                return 1;
            }
            0
        });
        Self { checks, text_transforms: text_transforms.map(|x| x.iter().map(
            |(f, v)| (f.to_owned(), format!("'{v}'")),
        ).collect()).unwrap_or_default() }
    }

    pub fn apply<'a, T>(
        &'a self,
        mut statement: T,
        lookup_table: &'a mut HashMap<String, HashSet<String>>,
    ) -> Result<Option<T>, anyhow::Error>
        where
            T: IntoIterator + Clone + Extend<(&'a String, &'a String)> + std::fmt::Debug,
            HashMap<String, (String, sqlparser::ast::DataType)>: FromIterator<<T>::Item>
    {
        let value_per_field: HashMap<String, (String, sqlparser::ast::DataType)> = statement.clone().into_iter().collect();

        if value_per_field.is_empty() {
            return Ok(Some(statement));
        }

        for check in self.checks.iter() {
            let col_name = check.get_column_name();
            let (str_value, data_type): &(String, sqlparser::ast::DataType) = &value_per_field[col_name];
            if !check.test_value(str_value, data_type, lookup_table)? {
                return Ok(None);
            }
        }

        statement.extend(self.text_transforms.iter());
        Ok(Some(statement))
    }
}

type PassChecks = HashMap<String, TableChecks>;

#[derive(Debug)]
pub struct DBChecks(pub Vec<PassChecks>);

impl DBChecks {
    fn new(items: Vec<Vec<Vec<PlainCheckType>>>, text_transforms: HashMap<String, HashMap<String, String>>) -> Self {
        Self(items.into_iter().map(|t_items| {
            t_items.into_iter().map(|it| {
                let table_name = it[0].get_table_name().to_owned();
                (table_name.to_string(), TableChecks::new(it, text_transforms.get(&table_name)))
            }).collect()
        }).collect())
    }

    /// Print the execution plan showing passes and tables to be processed
    pub fn print_plan(&self) {
        println!("=== Execution Plan ===");
        println!("Total passes: {}", self.0.len());
        println!();
        for (pass_idx, pass_checks) in self.0.iter().enumerate() {
            println!("Pass {}:", pass_idx + 1);
            for (table, table_checks) in pass_checks {
                println!("  Table: {}", table);
                for check in &table_checks.checks {
                    println!("    - {}", check.get_key());
                }
            }
        }
        println!("======================");
        println!();
    }
}

impl IntoIterator for DBChecks {
    type Item = PassChecks;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

fn new_plain_test(table: &str, definition: &str) -> Result<PlainCheckType, anyhow::Error> {
    let item: PlainCheckType = if definition.contains("->") {
        Box::new(PlainLookupTest::new(definition, table)?)
    } else {
        Box::new(PlainCelTest::new(definition, table)?)
    };
    Ok(item)
}

fn new_tracking_test(table: &str, definition: &str) -> Result<PlainCheckType, anyhow::Error> {
    Ok(Box::new(PlainTrackingTest::new(definition, table)?))
}


fn determine_foreign_keys(definition: &str) -> Result<Vec<String>, anyhow::Error> {
    let (_, foreign_keys) = if definition.contains("->") {
        PlainLookupTest::get_column_info(definition)?
    } else {
        PlainCelTest::get_column_info(definition)?
    };
    Ok(foreign_keys)
}

fn split_column_key(key: &str) -> Result<(&str, &str), anyhow::Error> {
    let mut split = key.split('.');
    let (Some(table), Some(column), None) = (split.next(), split.next(), split.next()) else {
        return Err(anyhow::anyhow!("malformed key {}", key));
    };
    Ok((table, column))
}

pub fn test_get_passes(definitions: &[(String, String)]) -> Result<(), anyhow::Error> {
    let mut root = DependencyNode::<CheckDefinition>::new();
    for (source_table, definition) in definitions.iter() {
        root.add_target(CheckDefinition(source_table.to_string(), definition.to_string()))?;

        for target_key in determine_foreign_keys(definition)? {
            let (target_table, target_column) = split_column_key(&target_key)?;

            root.add_target(CheckDefinition(target_table.to_string(), target_column.to_string()))?;

            root.add_dependency(target_table, source_table)?;
        }
    }

    let _chunked = chunk_by_depth(root);

    Ok(())
}

/// Build database checks from configuration conditions
///
/// Analyzes filter and cascade definitions to create a multi-pass processing plan
/// that respects dependency relationships between tables.
///
/// # Arguments
/// * `conditions` - Iterator over table names and their associated filter/cascade definitions
/// * `text_transforms` - Text replacement rules for data anonymization
///
/// # Returns
/// * `Ok(DBChecks)` containing the organized processing passes
/// * `Err(anyhow::Error)` if dependency analysis fails
pub fn get_passes<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(conditions: I, text_transforms: HashMap<String, HashMap<String, String>>) -> Result<DBChecks, anyhow::Error> {
    let definitions: Vec<(String, String)> = conditions.flat_map(|(table, conds)| {
        conds.iter().map(|c| (table.to_owned(), c.to_owned()))
    }).collect();

    test_get_passes(&definitions)?;
    let mut root = DependencyNode::<PlainCheckType>::new();
    for (source_table, definition) in definitions.iter() {
        root.add_child_to_group(new_plain_test(source_table, definition)?, source_table)?;

        for target_key in determine_foreign_keys(definition)? {
            let (target_table, _) = split_column_key(&target_key)?;

            let target_check = new_tracking_test(target_table, &target_key)?;
            root.add_child_to_group(target_check, target_table)?;

            root.add_dependency(target_table, source_table)?;
        }
    }

    let chunked = chunk_by_depth(root);

    let db_checks = DBChecks::new(chunked, text_transforms);

    Ok(db_checks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use chrono::NaiveDateTime;

    fn create_test_data_type() -> sqlparser::ast::DataType {
        sqlparser::ast::DataType::Int(None)
    }

    fn create_test_varchar_data_type() -> sqlparser::ast::DataType {
        sqlparser::ast::DataType::Varchar(Some(sqlparser::ast::CharacterLength::IntegerLength { length: 255, unit: None }))
    }

    fn create_test_datetime_data_type() -> sqlparser::ast::DataType {
        sqlparser::ast::DataType::Datetime(None)
    }

    #[test]
    fn test_value_parse_int() {
        let value = Value::parse("123", &create_test_data_type());
        match value {
            Value::Int(i) => assert_eq!(i, 123),
            _ => panic!("Expected Int value"),
        }
    }

    #[test]
    fn test_value_parse_string() {
        let value = Value::parse("'hello world'", &create_test_varchar_data_type());
        match value {
            Value::String(s) => assert_eq!(s, "hello world"),
            _ => panic!("Expected String value"),
        }
    }

    #[test]
    fn test_value_parse_null() {
        let value = Value::parse("NULL", &create_test_data_type());
        match value {
            Value::Null => {},
            _ => panic!("Expected Null value"),
        }
    }

    #[test]
    fn test_value_parse_date() {
        let value = Value::parse("'2023-12-25 10:30:00'", &create_test_datetime_data_type());
        match value {
            Value::Date(timestamp) => {
                let expected = NaiveDateTime::parse_from_str("2023-12-25 10:30:00", "%Y-%m-%d %H:%M:%S")
                    .unwrap()
                    .and_utc()
                    .timestamp();
                assert_eq!(timestamp, expected);
            },
            _ => panic!("Expected Date value"),
        }
    }

    #[test]
    fn test_value_parse_date_short() {
        let value = Value::parse("'2023-12-25'", &create_test_datetime_data_type());
        match value {
            Value::Date(timestamp) => {
                let expected = NaiveDateTime::parse_from_str("2023-12-25 00:00:00", "%Y-%m-%d %H:%M:%S")
                    .unwrap()
                    .and_utc()
                    .timestamp();
                assert_eq!(timestamp, expected);
            },
            _ => panic!("Expected Date value"),
        }
    }

    #[test]
    fn test_value_parse_zero_date() {
        let value = Value::parse("'0000-00-00 00:00:00'", &create_test_datetime_data_type());
        match value {
            Value::Date(timestamp) => {
                assert_eq!(timestamp, NaiveDateTime::MIN.and_utc().timestamp());
            },
            _ => panic!("Expected Date value"),
        }
    }

    #[test]
    fn test_value_parse_int_functions() {
        assert_eq!(Value::parse_int("42"), 42);
        assert_eq!(Value::parse_int("0"), 0);
        assert_eq!(Value::parse_int("-123"), -123);
    }

    #[test]
    #[should_panic(expected = "cannot parse int")]
    fn test_value_parse_int_invalid() {
        Value::parse_int("not_a_number");
    }

    #[test]
    fn test_value_parse_string_function() {
        assert_eq!(Value::parse_string("'hello'"), "hello");
        assert_eq!(Value::parse_string("'test''quoted'"), "testquoted");
        assert_eq!(Value::parse_string("unquoted"), "unquoted");
    }

    #[test]
    fn test_plain_cel_test_new() {
        let result = PlainCelTest::new("id > 10", "users");
        assert!(result.is_ok());

        let test = result.unwrap();
        assert_eq!(test.get_table_name(), "users");
        assert_eq!(test.get_column_name(), "id");
        assert_eq!(test.get_definition(), "id > 10");
        assert!(test.get_key().contains("cel"));
        assert!(test.get_key().contains("users"));
    }

    #[test]
    fn test_plain_cel_test_test_value_true() {
        let test = PlainCelTest::new("id > 10", "users").unwrap();
        let mut lookup_table = HashMap::new();

        let result = test.test_value("15", &create_test_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_plain_cel_test_test_value_false() {
        let test = PlainCelTest::new("id > 10", "users").unwrap();
        let mut lookup_table = HashMap::new();

        let result = test.test_value("5", &create_test_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_plain_cel_test_with_string() {
        let test = PlainCelTest::new("name == 'John'", "users").unwrap();
        let mut lookup_table = HashMap::new();

        let result = test.test_value("'John'", &create_test_varchar_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        let result = test.test_value("'Jane'", &create_test_varchar_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_plain_cel_test_get_column_info() {
        let result = PlainCelTest::get_column_info("id > 10");
        assert!(result.is_ok());

        let (column_name, _) = result.unwrap();
        assert_eq!(column_name, "id");
    }

    #[test]
    fn test_plain_lookup_test_new() {
        let result = PlainLookupTest::new("user_id->users.id", "profiles");
        assert!(result.is_ok());

        let test = result.unwrap();
        assert_eq!(test.get_table_name(), "profiles");
        assert_eq!(test.get_column_name(), "user_id");
        assert_eq!(test.get_definition(), "user_id->users.id");
        assert!(test.get_key().contains("lookup"));
    }

    #[test]
    fn test_plain_lookup_test_test_value_exists() {
        let test = PlainLookupTest::new("user_id->users.id", "profiles").unwrap();
        let mut lookup_table = HashMap::new();
        lookup_table.insert("users.id".to_string(), HashSet::from(["1".to_string(), "2".to_string()]));

        let result = test.test_value("1", &create_test_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_plain_lookup_test_test_value_not_exists() {
        let test = PlainLookupTest::new("user_id->users.id", "profiles").unwrap();
        let mut lookup_table = HashMap::new();
        lookup_table.insert("users.id".to_string(), HashSet::from(["1".to_string(), "2".to_string()]));

        let result = test.test_value("3", &create_test_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_plain_lookup_test_test_value_no_lookup_table() {
        let test = PlainLookupTest::new("user_id->users.id", "profiles").unwrap();
        let mut lookup_table = HashMap::new();

        let result = test.test_value("1", &create_test_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should allow when lookup table doesn't exist
    }

    #[test]
    fn test_plain_lookup_test_get_column_info() {
        let result = PlainLookupTest::get_column_info("user_id->users.id");
        assert!(result.is_ok());

        let (column_name, foreign_keys) = result.unwrap();
        assert_eq!(column_name, "user_id");
        assert_eq!(foreign_keys.len(), 1);
        assert_eq!(foreign_keys[0], "users.id");
    }

    #[test]
    #[should_panic(expected = "cannot parse cascade")]
    fn test_plain_lookup_test_invalid_format() {
        PlainLookupTest::new("invalid_format", "profiles").unwrap();
    }

    #[test]
    fn test_plain_tracking_test_new() {
        let result = PlainTrackingTest::new("users.id", "users");
        assert!(result.is_ok());

        let test = result.unwrap();
        assert_eq!(test.get_table_name(), "users");
        assert_eq!(test.get_column_name(), "id");
        assert_eq!(test.get_definition(), "users.id");
        assert!(test.get_key().contains("track"));
    }

    #[test]
    fn test_plain_tracking_test_test_value_first_time() {
        let test = PlainTrackingTest::new("users.id", "users").unwrap();
        let mut lookup_table = HashMap::new();

        let result = test.test_value("123", &create_test_data_type(), &mut lookup_table);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        // Check that value was added to lookup table
        assert!(lookup_table.contains_key("users.id"));
        assert!(lookup_table["users.id"].contains("123"));
    }

    #[test]
    fn test_plain_tracking_test_test_value_multiple_times() {
        let test = PlainTrackingTest::new("users.id", "users").unwrap();
        let mut lookup_table = HashMap::new();

        // Add first value
        test.test_value("123", &create_test_data_type(), &mut lookup_table).unwrap();
        // Add second value
        test.test_value("456", &create_test_data_type(), &mut lookup_table).unwrap();
        // Add duplicate value
        test.test_value("123", &create_test_data_type(), &mut lookup_table).unwrap();

        let tracked_values = &lookup_table["users.id"];
        assert_eq!(tracked_values.len(), 2); // Should have unique values only
        assert!(tracked_values.contains("123"));
        assert!(tracked_values.contains("456"));
    }

    #[test]
    fn test_plain_tracking_test_get_tracked_columns() {
        let test = PlainTrackingTest::new("users.id", "users").unwrap();
        let tracked = test.get_tracked_columns();
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0], "users.id");
    }

    #[test]
    fn test_plain_tracking_test_table_mismatch() {
        let result = PlainTrackingTest::new("orders.id", "users"); // Different table name
        assert!(result.is_err());
    }

    #[test]
    fn test_table_checks_new() {
        let checks: Vec<PlainCheckType> = vec![
            Box::new(PlainCelTest::new("id > 0", "users").unwrap()),
            Box::new(PlainTrackingTest::new("users.id", "users").unwrap()),
        ];

        let table_checks = TableChecks::new(checks, None);
        assert_eq!(table_checks.checks.len(), 2);

        dbg!(&table_checks.checks);
        // Tracking tests should be sorted last
        assert!(table_checks.checks[0].as_any().downcast_ref::<PlainCelTest>().is_some());
        assert!(table_checks.checks[1].as_any().downcast_ref::<PlainTrackingTest>().is_some());
    }

    #[test]
    fn test_table_checks_with_text_transforms() {
        let checks: Vec<PlainCheckType> = vec![
            Box::new(PlainCelTest::new("id > 0", "users").unwrap()),
        ];

        let text_transforms = HashMap::from([
            ("email".to_string(), "anonymized@example.com".to_string()),
            ("phone".to_string(), "555-0000".to_string()),
        ]);

        let table_checks = TableChecks::new(checks, Some(&text_transforms));

        // Check that text transforms are properly formatted with quotes
        assert_eq!(table_checks.text_transforms.len(), 2);
        assert_eq!(table_checks.text_transforms["email"], "'anonymized@example.com'");
        assert_eq!(table_checks.text_transforms["phone"], "'555-0000'");
    }

    #[test]
    fn test_new_plain_test_cel() {
        let result = new_plain_test("users", "id > 10");
        assert!(result.is_ok());

        let test = result.unwrap();
        assert_eq!(test.get_table_name(), "users");
        assert!(test.get_key().contains("cel"));
    }

    #[test]
    fn test_new_plain_test_lookup() {
        let result = new_plain_test("profiles", "user_id->users.id");
        assert!(result.is_ok());

        let test = result.unwrap();
        assert_eq!(test.get_table_name(), "profiles");
        assert!(test.get_key().contains("lookup"));
    }

    #[test]
    fn test_new_tracking_test() {
        let result = new_tracking_test("users", "users.id");
        assert!(result.is_ok());

        let test = result.unwrap();
        assert_eq!(test.get_table_name(), "users");
        assert!(test.get_key().contains("track"));
    }

    #[test]
    fn test_determine_foreign_keys_cel() {
        let result = determine_foreign_keys("id > 10");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_determine_foreign_keys_lookup() {
        let result = determine_foreign_keys("user_id->users.id");
        assert!(result.is_ok());
        let foreign_keys = result.unwrap();
        assert_eq!(foreign_keys.len(), 1);
        assert_eq!(foreign_keys[0], "users.id");
    }

    #[test]
    fn test_split_column_key() {
        let result = split_column_key("users.id");
        assert!(result.is_ok());
        let (table, column) = result.unwrap();
        assert_eq!(table, "users");
        assert_eq!(column, "id");
    }

    #[test]
    fn test_split_column_key_invalid() {
        let result = split_column_key("invalid");
        assert!(result.is_err());

        let result = split_column_key("too.many.parts");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_passes_simple() {
        let mut conditions = HashMap::new();
        conditions.insert("users".to_string(), vec!["id > 10".to_string()]);

        let text_transforms = HashMap::new();

        let result = get_passes(conditions.iter(), text_transforms);
        assert!(result.is_ok());

        let db_checks = result.unwrap();
        assert!(!db_checks.0.is_empty());
    }

    #[test]
    fn test_get_passes_with_cascades() {
        let mut conditions = HashMap::new();
        conditions.insert("users".to_string(), vec!["id > 10".to_string()]);
        conditions.insert("profiles".to_string(), vec!["user_id->users.id".to_string()]);

        let text_transforms = HashMap::new();

        let result = get_passes(conditions.iter(), text_transforms);
        assert!(result.is_ok());

        let db_checks = result.unwrap();
        // Should have multiple passes due to dependencies
        assert!(!db_checks.0.is_empty());
    }

    #[test]
    fn test_get_passes_with_text_transforms() {
        let mut conditions = HashMap::new();
        conditions.insert("users".to_string(), vec!["id > 10".to_string()]);

        let mut text_transforms = HashMap::new();
        let mut user_transforms = HashMap::new();
        user_transforms.insert("email".to_string(), "anonymized@example.com".to_string());
        text_transforms.insert("users".to_string(), user_transforms);

        let result = get_passes(conditions.iter(), text_transforms);
        assert!(result.is_ok());

        let db_checks = result.unwrap();
        assert!(!db_checks.0.is_empty());
    }

    // Mock implementation for testing SqlStatement-like objects
    #[derive(Debug, Clone)]
    struct MockStatement {
        data: HashMap<String, (String, sqlparser::ast::DataType)>,
    }

    impl IntoIterator for MockStatement {
        type Item = (String, (String, sqlparser::ast::DataType));
        type IntoIter = std::collections::hash_map::IntoIter<String, (String, sqlparser::ast::DataType)>;

        fn into_iter(self) -> Self::IntoIter {
            self.data.into_iter()
        }
    }

    impl<'a> Extend<(&'a String, &'a String)> for MockStatement {
        fn extend<T: IntoIterator<Item = (&'a String, &'a String)>>(&mut self, iter: T) {
            for (key, value) in iter {
                // For testing, we'll use a simple varchar type
                self.data.insert(key.clone(), (value.clone(), create_test_varchar_data_type()));
            }
        }
    }

    // impl std::iter::FromIterator<(String, (String, sqlparser::ast::DataType))> for HashMap<String, (String, sqlparser::ast::DataType)> {
    //     fn from_iter<T: IntoIterator<Item = (String, (String, sqlparser::ast::DataType))>>(iter: T) -> Self {
    //         iter.into_iter().collect()
    //     }
    // }

    #[test]
    fn test_table_checks_apply_pass() {
        let checks: Vec<PlainCheckType> = vec![
            Box::new(PlainCelTest::new("id > 0", "users").unwrap()),
        ];

        let table_checks = TableChecks::new(checks, None);
        let mut lookup_table = HashMap::new();

        let mut statement = MockStatement {
            data: HashMap::from([
                ("id".to_string(), ("10".to_string(), create_test_data_type())),
            ]),
        };

        let result = table_checks.apply(statement, &mut lookup_table);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_table_checks_apply_fail() {
        let checks: Vec<PlainCheckType> = vec![
            Box::new(PlainCelTest::new("id > 10", "users").unwrap()),
        ];

        let table_checks = TableChecks::new(checks, None);
        let mut lookup_table = HashMap::new();

        let statement = MockStatement {
            data: HashMap::from([
                ("id".to_string(), ("5".to_string(), create_test_data_type())),
            ]),
        };

        let result = table_checks.apply(statement, &mut lookup_table);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should be filtered out
    }

    #[test]
    fn test_table_checks_apply_with_transforms() {
        let checks: Vec<PlainCheckType> = vec![
            Box::new(PlainCelTest::new("id > 0", "users").unwrap()),
        ];

        let text_transforms = HashMap::from([
            ("email".to_string(), "anonymized@example.com".to_string()),
        ]);

        let table_checks = TableChecks::new(checks, Some(&text_transforms));
        let mut lookup_table = HashMap::new();

        let mut statement = MockStatement {
            data: HashMap::from([
                ("id".to_string(), ("10".to_string(), create_test_data_type())),
            ]),
        };

        let result = table_checks.apply(statement, &mut lookup_table);
        assert!(result.is_ok());
        let modified_statement = result.unwrap().unwrap();

        // Check that text transforms were applied
        assert!(modified_statement.data.contains_key("email"));
        assert_eq!(modified_statement.data["email"].0, "'anonymized@example.com'");
    }

    #[test]
    fn test_table_checks_apply_empty_statement() {
        let checks: Vec<PlainCheckType> = vec![
            Box::new(PlainCelTest::new("id > 0", "users").unwrap()),
        ];

        let table_checks = TableChecks::new(checks, None);
        let mut lookup_table = HashMap::new();

        let statement = MockStatement {
            data: HashMap::new(), // Empty statement
        };

        let result = table_checks.apply(statement, &mut lookup_table);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Empty statements should pass through
    }
}

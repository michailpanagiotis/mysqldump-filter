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

impl<'a> From<&'a PlainCheckType> for &'a str {
    fn from(item: &'a PlainCheckType) -> Self {
        item.get_key()
    }
}

impl core::fmt::Debug for dyn PlainColumnCheck {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        (self.get_key()).fmt(f)
    }
}

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
pub struct TableChecks(Vec<PlainCheckType>);

impl TableChecks {
    pub fn apply<T>(
        &self,
        mut statement: T,
        lookup_table: &mut HashMap<String, HashSet<String>>,
    ) -> Result<Option<T>, anyhow::Error>
        where
            T: IntoIterator + Clone + Extend<(String, String)> + std::fmt::Debug,
            HashMap<String, (String, sqlparser::ast::DataType)>: FromIterator<<T>::Item>
    {
        let value_per_field: HashMap<String, (String, sqlparser::ast::DataType)> = statement.clone().into_iter().collect();

        if value_per_field.is_empty() {
            return Ok(Some(statement));
        }

        for check in self.0.iter() {
            let col_name = check.get_column_name();
            let (str_value, data_type): &(String, sqlparser::ast::DataType) = &value_per_field[col_name];
            if !check.test_value(str_value, data_type, lookup_table)? {
                return Ok(None);
            }
        }

        statement.extend(HashMap::new());
        Ok(Some(statement))
    }
}

impl From<Vec<PlainCheckType>> for TableChecks {
    fn from(items: Vec<PlainCheckType>) -> Self {
        let mut res = Self(items);
        // tests have implicit order
        res.0.sort_by_key(|a| {
            if a.as_any().downcast_ref::<PlainTrackingTest>().is_some() {
                return true;
            }
            false
        });
        res
    }
}

type PassChecks = HashMap<String, TableChecks>;

#[derive(Debug)]
pub struct DBChecks(pub Vec<PassChecks>);

impl From<Vec<Vec<Vec<PlainCheckType>>>> for DBChecks {
    fn from(items: Vec<Vec<Vec<PlainCheckType>>>) -> Self {
        Self(items.into_iter().map(|t_items| {
            t_items.into_iter().map(|it| (it[0].get_table_name().to_string(), TableChecks::from(it))).collect()
        }).collect())
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

pub fn get_passes<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(conditions: I) -> Result<DBChecks, anyhow::Error> {
    let definitions: Vec<(String, String)> = conditions.flat_map(|(table, conds)| {
        conds.iter().map(|c| (table.to_owned(), c.to_owned()))
    }).collect();

    let mut root = DependencyNode::<PlainCheckType>::new();
    for (source_table, definition) in definitions.iter() {
        root.add_child_to_group(new_plain_test(source_table, definition)?, source_table)?;

        for target_key in determine_foreign_keys(definition)? {
            let (target_table, _) = split_column_key(&target_key)?;

            let target_check = new_tracking_test(target_table, &target_key)?;
            root.add_child_to_group(target_check, target_table)?;

            root.move_under(target_table, source_table)?;
        }
    }


    let db_checks = DBChecks::from(chunk_by_depth(root));
    dbg!(&db_checks);

    Ok(db_checks)
}

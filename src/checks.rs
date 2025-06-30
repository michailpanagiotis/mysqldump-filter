use cel_interpreter::{Context, Program};
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::dependencies::DependencyNode;

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

    fn test(
        &mut self,
        value: &str,
        data_type: &sqlparser::ast::DataType,
        lookup_table: &HashMap<String, HashSet<String>>,
    ) -> Result<bool, anyhow::Error>;

    fn get_table_name(&self) -> &str;

    fn get_column_name(&self) -> &str;

    fn get_definition(&self) -> &str;

    fn get_key(&self) -> &str;
}

impl<'a> Into<&'a str> for &'a PlainCheckType {
    fn into(self) -> &'a str {
        self.get_key()
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
            definition: definition.to_owned(),
            program,
        })
    }

    fn test(
        &mut self,
        value: &str,
        data_type: &sqlparser::ast::DataType,
        _lookup_table: &HashMap<String, HashSet<String>>,
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
}

#[derive(Debug)]
pub struct PlainLookupTest {
    key: String,
    table_name: String,
    column_name: String,
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
            definition: definition.to_owned(),
            target_column_key: foreign_key.to_owned(),
        })
    }

    fn test(
        &mut self,
        value: &str,
        _data_type: &sqlparser::ast::DataType,
        lookup_table: &HashMap<String, HashSet<String>>,
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
}

#[derive(Debug)]
pub struct PlainTrackingTest {
    key: String,
    table_name: String,
    column_name: String,
    definition: String,
    values: HashSet<String>,
}

impl PlainTrackingTest {
    pub fn get_column_info(definition: &str) -> Result<(String, Vec<String>), anyhow::Error> {
        let mut split = definition.split("->");
        let (Some(column_name), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };
        Ok((column_name.to_owned(), Vec::from([foreign_key.to_owned()])))
    }
}

impl PlainColumnCheck for PlainTrackingTest {
    fn new(definition: &str, table: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized {
        let mut split = definition.split(".");
        let (Some(table), Some(column), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse test");
        };

        Ok(PlainTrackingTest {
            key: String::from("track: ") + table + ": " + definition,
            table_name: table.to_owned(),
            column_name: column.to_owned(),
            definition: definition.to_owned(),
            values: HashSet::new(),
        })
    }

    fn test(
        &mut self,
        value: &str,
        _data_type: &sqlparser::ast::DataType,
        _lookup_table: &HashMap<String, HashSet<String>>,
    ) -> Result<bool, anyhow::Error> {
        self.values.insert(value.to_string());
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
}

pub fn new_plain_test(table: &str, definition: &str) -> Result<PlainCheckType, anyhow::Error> {
    let item: PlainCheckType = if definition.contains("->") {
        Box::new(PlainLookupTest::new(definition, table)?)
    } else {
        Box::new(PlainCelTest::new(definition, table)?)
    };
    Ok(item)
}

pub fn new_tracking_test(table: &str, definition: &str) -> Result<PlainCheckType, anyhow::Error> {
    Ok(Box::new(PlainTrackingTest::new(definition, table)?))
}

pub fn parse_test_definition(definition: &str) -> Result<(String, Vec<String>), anyhow::Error> {
    let (column_name, foreign_keys) = if definition.contains("->") {
        PlainLookupTest::get_column_info(definition)?
    } else {
        PlainCelTest::get_column_info(definition)?
    };
    Ok((column_name, foreign_keys))
}

pub fn determine_target_tables(definition: &str) -> Result<Vec<String>, anyhow::Error> {
    let mut target_tables = Vec::new();
    let (_, deps) = parse_test_definition(definition)?;
    for key in deps.iter() {
        let mut split = key.split('.');
        let (Some(target_table), Some(_), None) = (split.next(), split.next(), split.next()) else {
            return Err(anyhow::anyhow!("malformed key {}", key));
        };
        target_tables.push(target_table.to_owned());
        target_tables.dedup();
    }
    Ok(target_tables)
}

fn determine_checks_per_table(definitions: &[(String, String)]) -> Result<HashMap<String, Vec<String>>, anyhow::Error> {
    let mut checks: HashMap<String, Vec<String>> = HashMap::new();
    for (table, definition) in definitions.iter() {
        if !checks.contains_key(table) {
            checks.insert(table.to_owned(), Vec::new());
        }

        let Some(t_checks) = checks.get_mut(table) else {
            return Err(anyhow::anyhow!("cannot get references of table"));
        };
        t_checks.push(definition.to_owned());
        t_checks.dedup();
    }
    Ok(checks)
}

fn determine_references_per_table(definitions: &[(String, String)]) -> Result<HashMap<String, Vec<String>>, anyhow::Error> {
    let mut references: HashMap<String, Vec<String>> = HashMap::new();
    for (table, definition) in definitions.iter() {
        let (_, deps) = parse_test_definition(definition)?;
        if !references.contains_key(table) {
            references.insert(table.to_owned(), Vec::new());
        }

        for key in deps.iter() {
            let mut split = key.split('.');
            dbg!(&key);
            let (Some(target_table), Some(_), None) = (split.next(), split.next(), split.next()) else {
                return Err(anyhow::anyhow!("malformed key {}", key));
            };
            if !references.contains_key(target_table) {
                references.insert(target_table.to_owned(), Vec::new());
            }

            let Some(refs) = references.get_mut(target_table) else {
                return Err(anyhow::anyhow!("cannot get references of table"));
            };
            refs.push(key.to_owned());
            refs.dedup();
        }
    }
    Ok(references)
}

fn determine_all_checked_tables(definitions: &[(String, String)]) -> Result<HashSet<String>, anyhow::Error> {
    let mut all_tables: HashSet<String> = HashSet::new();
    for (table, definition) in definitions.iter() {
        all_tables.insert(table.to_owned());
        let target_tables = determine_target_tables(definition)?;
        for target_table in target_tables {
            all_tables.insert(target_table.to_owned());
        }
    }
    Ok(all_tables)
}

#[derive(Debug)]
pub struct TableChecks {
    pub references: Vec<String>,
    pub checks: Vec<PlainCheckType>,
}

impl TableChecks {
    pub fn new(check_definitions: Vec<PlainCheckType>, references: &[String]) -> Result<Self, anyhow::Error> {
        Ok(TableChecks {
            references: Vec::from(references),
            checks: check_definitions,
        })
    }
}

pub fn get_passes(definitions: &[(String, String)]) -> Result<Vec<HashMap<String, Vec<PlainCheckType>>>, anyhow::Error> {
    let mut root = DependencyNode::<PlainCheckType>::new();
    for (source_table, definition) in definitions.iter() {
        let (_, foreign_keys) = parse_test_definition(definition)?;
        let new_check = new_plain_test(source_table, definition)?;

        root.add_child_to_group(new_check, source_table)?;

        for target_key in foreign_keys {
            let mut split = target_key.split('.');
            let (Some(target_table), Some(_), None) = (split.next(), split.next(), split.next()) else {
                return Err(anyhow::anyhow!("malformed key {}", target_key));
            };

            let target_check = new_tracking_test(target_table, &target_key)?;
            root.add_child_to_group(target_check, target_table)?;

            root.move_under(target_table, source_table)?;
        }
    }

    dbg!(&root);

    Ok(root.chunk_by_depth())
}

pub fn test_checks(
    checks: &mut [PlainCheckType],
    value_per_field: &HashMap<String, (String, sqlparser::ast::DataType)>,
    lookup_table: &HashMap<String, HashSet<String>>,
) -> Result<bool, anyhow::Error> {
    for check in checks.iter_mut() {
        let col_name = check.get_column_name().to_owned();
        let (str_value, data_type): &(String, sqlparser::ast::DataType) = &value_per_field[&col_name];
        if !check.test(str_value, data_type, lookup_table)? {
            return Ok(false);
        }
    }
    Ok(true)
}

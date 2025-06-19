use cel_interpreter::{Context, Program};
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::scanner::Value;

pub trait PlainColumnCheck {
    fn new(definition: &str, table: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized;

    fn test(
        &self,
        column_name: &str,
        value: &Value,
        lookup_table: &HashMap<String, HashSet<String>>,
    ) -> bool;

    fn get_table_name(&self) -> &str;

    fn get_column_name(&self) -> &str;

    fn get_definition(&self) -> &str;
}

impl core::fmt::Debug for dyn PlainColumnCheck {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        (self.get_table_name().to_string() + ": " + self.get_definition()).fmt(f)
    }
}

#[derive(Debug)]
pub struct PlainCelTest {
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

    fn build_context(&self, column_name: &str, value: &Value) -> Context {
        let mut context = Context::default();
        context.add_function("timestamp", |d: Arc<String>| {
            PlainCelTest::parse_date(&d)
        });

        match value {
            Value::Int { parsed, .. } => context.add_variable(column_name, parsed),
            Value::Date { parsed, .. } => context.add_variable(column_name, parsed),
            Value::String { parsed, .. } => context.add_variable(column_name, parsed),
            Value::Null { .. } => context.add_variable(column_name, false),
        };

        context
    }
}

impl PlainColumnCheck for PlainCelTest {
    fn new(definition: &str, table: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];

        Ok(PlainCelTest {
            table_name: table.to_owned(),
            column_name: column.to_owned(),
            definition: definition.to_owned(),
            program,
        })
    }

    fn test(
        &self,
        column_name: &str,
        value: &Value,
        _lookup_table: &HashMap<String, HashSet<String>>,
    ) -> bool {
        let context = self.build_context(column_name, value);
        match self.program.execute(&context).unwrap() {
            cel_interpreter::objects::Value::Bool(v) => {
                // println!("testing {}.{} {} -> {}", self.table, self.column, &other_value, &v);
                v
            }
            _ => panic!("filter does not return a boolean"),
        }
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
            table_name: table.to_owned(),
            column_name: source_column.to_owned(),
            definition: definition.to_owned(),
            target_column_key: foreign_key.to_owned(),
        })
    }

    fn test(
        &self,
        _column_name: &str,
        value: &Value,
        lookup_table: &HashMap<String, HashSet<String>>,
    ) -> bool {
        let Some(set) = lookup_table.get(&self.target_column_key) else { return true };
        set.contains(value.as_string())
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

pub fn parse_test_definition(definition: &str) -> Result<(String, Vec<String>), anyhow::Error> {
    let (column_name, foreign_keys) = if definition.contains("->") {
        PlainLookupTest::get_column_info(definition)?
    } else {
        PlainCelTest::get_column_info(definition)?
    };
    Ok((column_name, foreign_keys))
}

pub type PlainCheckType = Box<dyn PlainColumnCheck>;

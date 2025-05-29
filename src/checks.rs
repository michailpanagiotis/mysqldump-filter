use cel_interpreter::{Context, Program};
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::traits::ColumnMeta;

pub trait PlainColumnCheck {
    fn new(definition: &str, table: &str) -> Result<impl PlainColumnCheck + 'static, anyhow::Error> where Self: Sized;

    fn test(&self, column_meta: &ColumnMeta, value:&str, lookup_table: &HashMap<String, HashSet<String>>) -> bool;

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
    pub fn resolve_column_meta(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<(ColumnMeta, Vec<String>), anyhow::Error> {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];
        let column_meta = ColumnMeta::new(table, column, &Vec::new(), data_types)?;
        Ok((column_meta, Vec::new()))
    }

    fn parse_int(s: &str) -> i64 {
        s.parse().unwrap_or_else(|_| panic!("cannot parse int {s}"))
    }

    fn parse_date(s: &str) -> i64 {
        let to_parse = if s.len() == 10 { s.to_owned() + " 00:00:00" } else { s.to_owned() };
        NaiveDateTime::parse_from_str(&to_parse, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| panic!("cannot parse timestamp {s}"))
            .and_utc()
            .timestamp()
    }

    fn build_context(&self, column_meta: &ColumnMeta, other_value: &str) -> Context {
        let mut context = Context::default();
        context.add_function("timestamp", |d: Arc<String>| {
            PlainCelTest::parse_date(&d)
        });

        let column_name = column_meta.get_column_name();
        let data_type = column_meta.get_data_type();

        if other_value == "NULL" {
            context.add_variable(column_name.to_owned(), false).unwrap();
            return context;
        }

        let _ = match data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                context.add_variable(column_name, PlainCelTest::parse_int(other_value))
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                context.add_variable(column_name, PlainCelTest::parse_date(other_value))
            },
            sqlparser::ast::DataType::Enum(_, _) => {
                context.add_variable(column_name, other_value)
            },
            _ => panic!("{}", format!("cannot parse {other_value} for {data_type}"))
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

    fn test(&self, column_meta: &ColumnMeta, value:&str, _lookup_table: &HashMap<String, HashSet<String>>) -> bool {
        let context = self.build_context(column_meta, value);
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
    pub fn resolve_column_meta(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<(ColumnMeta, Vec<String>), anyhow::Error> {
        let mut split = definition.split("->");
        let (Some(source_column), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };

        let mut split = foreign_key.split('.');
        let (Some(target_table), Some(target_column), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {foreign_key}");
        };
        let target_column_meta = ColumnMeta::new(target_table, target_column, &Vec::new(), data_types)?;

        let column_meta = ColumnMeta::new(table, source_column, &Vec::from([target_column_meta.get_column_key()]), data_types)?;
        Ok((column_meta, Vec::from([target_column_meta.get_column_key().to_owned()])))
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

    fn test(&self, _column_meta: &ColumnMeta, value:&str, lookup_table: &HashMap<String, HashSet<String>>) -> bool {
        let Some(set) = lookup_table.get(&self.target_column_key) else { return true };
        set.contains(value)
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

pub type PlainCheckType = Box<dyn PlainColumnCheck>;

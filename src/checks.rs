use cel_interpreter::{Context, Program};
use cel_interpreter::extractors::This;
use chrono::NaiveDateTime;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::sql::{get_column_positions, get_values};

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

impl ColumnMeta {
    fn new(table: &str, column: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
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
}

pub trait TestValue {
    fn get_column_meta(&self) -> &ColumnMeta;
    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta;
    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool;

    fn get_column_name(&self) -> &str {
        &self.get_column_meta().column
    }

    fn get_data_type(&self) -> &sqlparser::ast::DataType {
        &self.get_column_meta().data_type
    }

    fn get_column_position(&self) -> &Option<usize> {
        &self.get_column_meta().position
    }

    fn get_dependencies(&self) -> HashSet<ColumnMeta> {
        HashSet::new()
    }

    fn has_resolved_position(&self) -> bool {
        self.get_column_meta().position.is_some()
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

    fn test_row(&self, values: &[&str], lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        self.get_column_position().is_some_and(|p| self.test(values[p], lookup_table))
    }
}

impl core::fmt::Debug for dyn TestValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.get_column_meta().fmt(f)
    }
}

#[derive(Debug)]
pub struct CelTest {
    column_meta: ColumnMeta,
    program: Program,
}

fn parse_date(s: &str) -> i64 {
    let to_parse = if s.len() == 10 { s.to_owned() + " 00:00:00" } else { s.to_owned() };
    let val = NaiveDateTime::parse_from_str(&to_parse, "%Y-%m-%d %H:%M:%S").unwrap_or_else(|_| panic!("cannot parse timestamp {s}"));
    let timestamp: i64 = val.and_utc().timestamp();
    timestamp
}

fn parse_int(s: &str) -> i64 {
    s.parse().unwrap_or_else(|_| panic!("cannot parse int {s}"))
}

fn timestamp(This(s): This<Arc<String>>) -> i64 {
    parse_date(&s)
}

impl CelTest {
    fn build_context(&self, other_value: &str) -> Context {
        let mut context = Context::default();
        context.add_function("timestamp", timestamp);

        let column_name = self.get_column_name();
        let data_type = self.get_data_type();

        if other_value == "NULL" {
            context.add_variable(column_name.to_owned(), false).unwrap();
            return context;
        }

        match data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                context.add_variable(column_name, parse_int(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                context.add_variable(column_name, parse_date(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Enum(_, _) => {
                context.add_variable(column_name, other_value).unwrap();
            },
            _ => panic!("{}", format!("cannot parse {other_value} for {data_type}"))
        };

        context
    }
}

impl TestValue for CelTest {
    fn get_column_meta(&self) -> &ColumnMeta {
        &self.column_meta
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        &mut self.column_meta
    }

    fn test(&self, value:&str, _lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        let context = self.build_context(value);
        match self.program.execute(&context).unwrap() {
            cel_interpreter::objects::Value::Bool(v) => {
                // println!("testing {}.{} {} -> {}", self.table, self.column, &other_value, &v);
                v
            }
            _ => panic!("filter does not return a boolean"),
        }
    }
}

impl CelTest {
    fn from_definition(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];

        CelTest {
            column_meta: ColumnMeta::new(table, column, data_types),
            program,
        }
    }
}

#[derive(Debug)]
pub struct LookupTest {
    column_meta: ColumnMeta,
    target_column_meta: ColumnMeta,
}

impl LookupTest {
    fn from_definition(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        let mut split = definition.split("->");
        let (Some(source_column), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };

        let mut split = foreign_key.split('.');
        let (Some(target_table), Some(target_column), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {foreign_key}");
        };

        LookupTest {
            column_meta: ColumnMeta::new(table, source_column, data_types),
            target_column_meta: ColumnMeta::new(target_table, target_column, data_types),
        }
    }

    pub fn get_target_column_meta(&self) -> &ColumnMeta {
        &self.target_column_meta
    }
}

impl TestValue for LookupTest {
    fn get_column_meta(&self) -> &ColumnMeta {
        &self.column_meta
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        &mut self.column_meta
    }

    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        let Some(fvs) = lookup_table else { return true };
        let Some(set) = fvs.get(&self.target_column_meta.key) else { return false };
        set.contains(value)
    }

    fn get_dependencies(&self) -> HashSet<ColumnMeta> {
        HashSet::from([self.get_target_column_meta().to_owned()])
    }
}

#[derive(Debug)]
pub struct RowCheck {
    checks: Vec<Box<dyn TestValue>>,
    tested_at_pass: Option<usize>,
    pending_dependencies: Vec<Weak<RefCell<RowCheck>>>,
}

impl RowCheck {
    pub fn from_config(table: &str, conditions: &[String], data_types: &HashMap<String, sqlparser::ast::DataType>) -> RowCheck {
        RowCheck {
            checks: conditions.iter().map(|condition| {
                let item: Box<dyn TestValue> = if condition.contains("->") {
                    Box::new(LookupTest::from_definition(condition, table, data_types))
                } else {
                    Box::new(CelTest::from_definition(condition, table, data_types))
                };
                item
                }).collect(),
            tested_at_pass: None,
            pending_dependencies: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.checks.is_empty()
    }

    pub fn has_resolved_positions(&self) -> bool {
        self.checks.iter().all(|t| {
            t.has_resolved_position()
        })
    }

    pub fn set_positions(&mut self, positions: HashMap<String, usize>) {
        for condition in self.checks.iter_mut() {
            condition.set_position_from_column_positions(&positions);
        }
        assert!(self.has_resolved_positions());
    }

    pub fn get_dependencies(&self) -> HashSet<ColumnMeta> {
        let mut dependencies = HashSet::new();
        for condition in self.checks.iter() {
            for dependency in condition.get_dependencies() {
                dependencies.insert(dependency);
            }
        }
        dependencies
    }

    pub fn link_dependencies(&mut self, per_table: &HashMap<String, Rc<RefCell<RowCheck>>>) {
        let deps = self.get_dependencies();
        for dep in deps {
            let target = &per_table[&dep.table];
            self.pending_dependencies.push(Rc::<RefCell<RowCheck>>::downgrade(target))
        }
    }

    pub fn is_ready_to_be_tested(&self) -> bool {
        self.pending_dependencies.iter().all(|d| {
            d.upgrade().unwrap().borrow().has_been_tested()
        })
    }

    pub fn has_been_tested(&self) -> bool {
        self.tested_at_pass.is_some()
    }

    pub fn test(&mut self, pass: &usize, insert_statement: &str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        if !self.is_ready_to_be_tested() {
            return true
        }
        if self.tested_at_pass.is_none() {
            self.tested_at_pass = Some(pass.to_owned());
        }
        if self.tested_at_pass.is_some_and(|x| &x < pass) {
            return true
        }
        let values = get_values(insert_statement);
        if !self.has_resolved_positions() {
            self.set_positions(get_column_positions(insert_statement));
        }
        self.checks.iter().all(|t| t.test_row(&values, lookup_table))
    }
}

pub fn from_config<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
    conditions: I,
    data_types: &HashMap<String, sqlparser::ast::DataType>,
) -> HashMap<String, Rc<RefCell<RowCheck>>> {
    let c: HashMap<String, Vec<String>> = conditions
        .flat_map(|(table, conditions)| conditions.iter().map(|c| (table.to_string(), c.to_owned())))
        .into_group_map();
    c.iter().map(|(table, definitions)| (table.to_owned(), Rc::new(RefCell::new(RowCheck::from_config(table, definitions, data_types))))).collect()
}

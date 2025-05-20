use cel_interpreter::{Context, Program};
use chrono::NaiveDateTime;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::traits::{ColumnMeta, ColumnPositions, DBColumn, ColumnTest, ReferenceTracker, NoDataTypeError};
use crate::sql::get_values;

#[derive(Debug)]
pub struct CelTest {
    column_meta: ColumnMeta,
    program: Program,
}

impl CelTest {
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

    fn build_context(&self, other_value: &str) -> Context {
        let mut context = Context::default();
        context.add_function("timestamp", |d: Arc<String>| {
            CelTest::parse_date(&d)
        });

        let column_name = self.get_column_name();
        let data_type = self.get_data_type();

        if other_value == "NULL" {
            context.add_variable(column_name.to_owned(), false).unwrap();
            return context;
        }

        let _ = match data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                context.add_variable(column_name, CelTest::parse_int(other_value))
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                context.add_variable(column_name, CelTest::parse_date(other_value))
            },
            sqlparser::ast::DataType::Enum(_, _) => {
                context.add_variable(column_name, other_value)
            },
            _ => panic!("{}", format!("cannot parse {other_value} for {data_type}"))
        };

        context
    }
}

impl DBColumn for CelTest {
    fn get_column_meta(&self) -> &ColumnMeta {
        &self.column_meta
    }
}

impl ColumnTest for CelTest {
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, NoDataTypeError> where Self: Sized {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];
        let column_meta = ColumnMeta::new(table, column, data_types)?;

        Ok(CelTest {
            column_meta,
            program,
        })
    }

    fn test(&self, value:&str, _lookup_table: &HashMap<String, HashSet<String>>) -> bool {
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

#[derive(Debug)]
pub struct LookupTest {
    column_meta: ColumnMeta,
    target_column_meta: ColumnMeta,
}

impl DBColumn for LookupTest {
    fn get_column_meta(&self) -> &ColumnMeta {
        &self.column_meta
    }
}

impl ColumnTest for LookupTest {
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, NoDataTypeError> where Self: Sized {
        let mut split = definition.split("->");
        let (Some(source_column), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };

        let column_meta = ColumnMeta::new(table, source_column, data_types)?;

        let mut split = foreign_key.split('.');
        let (Some(target_table), Some(target_column), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {foreign_key}");
        };

        Ok(LookupTest {
            column_meta,
            target_column_meta: ColumnMeta::new(target_table, target_column, data_types)?,
        })
    }

    fn test(&self, value:&str, lookup_table: &HashMap<String, HashSet<String>>) -> bool {
        let Some(set) = lookup_table.get(&self.target_column_meta.key) else { return true };
        set.contains(value)
    }

    fn get_column_dependencies(&self) -> HashSet<ColumnMeta> {
        HashSet::from([self.target_column_meta.to_owned()])
    }
}

#[derive(Debug)]
pub struct RowCheck {
    table: String,
    // trait ColumnPositions
    column_positions: Option<HashMap<String, usize>>,
    // trait ReferenceTracker
    referenced_columns: HashSet<ColumnMeta>,
    references: HashMap<String, HashSet<String>>,
    // trait DependencyTree
    tested_at_pass: Option<usize>,
    pending_dependencies: Vec<Weak<RefCell<RowCheck>>>,
    checks: Vec<Box<dyn ColumnTest>>,
}

impl ColumnPositions for RowCheck {
    fn get_column_positions(&self) -> &Option<HashMap<String, usize>> {
        &self.column_positions
    }

    fn set_column_positions(&mut self, positions: HashMap<String, usize>) {
        self.column_positions = Some(positions.to_owned());
    }
}

impl ReferenceTracker for RowCheck {
    fn get_referenced_columns(&self) -> &HashSet<ColumnMeta> {
        &self.referenced_columns
    }

    fn get_referenced_columns_mut(&mut self) -> &mut HashSet<ColumnMeta> {
        &mut self.referenced_columns
    }

    fn get_references(&self) -> &HashMap<String, HashSet<String>> {
        &self.references
    }

    fn get_references_mut(&mut self) -> &mut HashMap<String, HashSet<String>> {
        &mut self.references
    }
}

impl RowCheck {
    pub fn from_config<'a>(table: &'a str, conditions: &'a [String], data_types: &'a HashMap<String, sqlparser::ast::DataType>) -> Result<RowCheck, NoDataTypeError> {
        let mut checks: Vec<Box<dyn ColumnTest>> = Vec::new();
        for condition in conditions.iter() {
            let item: Box<dyn ColumnTest> = if condition.contains("->") {
                Box::new(LookupTest::new(condition, table, data_types)?)
            } else {
                Box::new(CelTest::new(condition, table, data_types)?)
            };
            checks.push(item);
        }

        Ok(RowCheck {
            table: table.to_owned(),
            column_positions: None,
            referenced_columns: HashSet::new(),
            references: HashMap::new(),
            checks,
            tested_at_pass: None,
            pending_dependencies: Vec::new(),
        })
    }


    pub fn get_column_dependencies(&self) -> HashSet<ColumnMeta> {
        let mut dependencies = HashSet::new();
        for condition in self.checks.iter() {
            for dependency in condition.get_column_dependencies() {
                dependencies.insert(dependency);
            }
        }
        dependencies
    }

    pub fn link_dependencies(&mut self, per_table: &HashMap<String, Rc<RefCell<RowCheck>>>) {
        for dep in self.get_column_dependencies() {
            self.add_dependency(&per_table[&dep.table]);
        }
    }

    pub fn set_fulfilled(&mut self, depth: &usize) {
        self.tested_at_pass = Some(depth.to_owned());
    }

    pub fn has_been_fulfilled(&self) -> bool {
        self.tested_at_pass.is_some()
    }

    pub fn get_dependencies(&self) -> &Vec<Weak<RefCell<Self>>> {
        &self.pending_dependencies
    }

    pub fn get_dependencies_mut(&mut self) -> &mut Vec<Weak<RefCell<Self>>> {
        &mut self.pending_dependencies
    }

    pub fn add_dependency(&mut self, target: &Rc<RefCell<RowCheck>>) {
        self.get_dependencies_mut().push(Rc::<RefCell<RowCheck>>::downgrade(target))
    }

    pub fn is_ready_to_be_tested(&self) -> bool {
        !self.has_been_fulfilled() && self.get_dependencies().iter().all(|d| {
            d.upgrade().unwrap().borrow().has_been_fulfilled()
        })
    }

    pub fn fulfill_dependency(&mut self, depth: &usize) {
        if !self.has_been_fulfilled() {
            self.set_fulfilled(depth);
        }
        assert!(self.has_been_fulfilled());
    }

    pub fn test(&mut self, pass: &usize, insert_statement: &str, lookup_table: &HashMap<String, HashSet<String>>) -> bool {
        self.fulfill_dependency(pass);

        self.resolve_column_positions(insert_statement);

        let values = get_values(insert_statement);

        let all_checks_passed = self.checks.iter().all(|t| {
            let value = values[self.get_column_position(t.get_column_name()).unwrap()];
            t.test(value, lookup_table)
        });

        if all_checks_passed {
            self.capture_references(&values);
        }

        all_checks_passed
    }
}

pub fn from_config<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
    conditions: I,
    data_types: &HashMap<String, sqlparser::ast::DataType>,
) -> Result<HashMap<String, Rc<RefCell<RowCheck>>>, NoDataTypeError> {
    let c: HashMap<String, Vec<String>> = conditions
        .flat_map(|(table, conditions)| conditions.iter().map(|c| (table.to_string(), c.to_owned())))
        .into_group_map();
    let mut result: HashMap<String, Rc<RefCell<RowCheck>>> = HashMap::new();
    for (table, definitions) in c.iter() {
        result.insert(table.to_owned(), Rc::new(RefCell::new(RowCheck::from_config(table, definitions, data_types)?)));
    }

    let deps: HashSet<ColumnMeta> = result.values().flat_map(|x| x.borrow().get_column_dependencies()).collect();

    for dep in deps.iter() {
        if result.contains_key(&dep.table) {
            result[&dep.table].borrow_mut().add_referenced_column(dep);
        } else {
            let mut row_check = RowCheck::from_config(&dep.table, &[], &HashMap::new())?;
            row_check.add_referenced_column(dep);
            result.insert(dep.table.to_owned(), Rc::new(RefCell::new(row_check)));
        }
    }

    for (_, row_check) in result.iter() {
        row_check.borrow_mut().link_dependencies(&result);
    }

    Ok(result)
}

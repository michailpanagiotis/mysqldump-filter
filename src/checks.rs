use cel_interpreter::{Context, Program};
use chrono::NaiveDateTime;
use itertools::Itertools;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::traits::{ColumnMeta, ColumnPositions, DBColumn, ColumnTest, ReferenceTracker, Dependency};
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
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, anyhow::Error> where Self: Sized {
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
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, anyhow::Error> where Self: Sized {
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
pub struct RowCheck<'a> {
    table: String,
    // trait ColumnPositions
    column_positions: Option<HashMap<String, usize>>,
    // trait ReferenceTracker
    referenced_columns: HashSet<ColumnMeta>,
    references: HashMap<String, HashSet<String>>,
    // trait Dependency
    tested_at_pass: Option<usize>,
    pending_dependencies: Vec<Weak<RefCell<dyn Dependency>>>,
    tracked_columns: &'a HashSet<ColumnMeta>,
    checks: &'a Vec<Box<dyn ColumnTest>>,
}

impl ColumnPositions for RowCheck<'_> {
    fn get_column_positions(&self) -> &Option<HashMap<String, usize>> {
        &self.column_positions
    }

    fn set_column_positions(&mut self, positions: HashMap<String, usize>) {
        self.column_positions = Some(positions.to_owned());
    }
}

impl ReferenceTracker for RowCheck<'_> {
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

impl Dependency for RowCheck<'_> {
    fn set_fulfilled_at_depth(&mut self, depth: &usize) {
        self.tested_at_pass = Some(depth.to_owned());
    }

    fn has_been_fulfilled(&self) -> bool {
        self.tested_at_pass.is_some()
    }

    fn get_dependencies(&self) -> &Vec<Weak<RefCell<dyn Dependency>>> {
        &self.pending_dependencies
    }

    fn get_dependencies_mut(&mut self) -> &mut Vec<Weak<RefCell<dyn Dependency>>> {
        &mut self.pending_dependencies
    }
}

impl<'a> RowCheck<'a> {
    pub fn from_config(table: &str, tracked_columns: &'a HashSet<ColumnMeta>, checks: &'a Vec<Box<dyn ColumnTest>>) -> Result<RowCheck<'a>, anyhow::Error> {
        Ok(RowCheck {
            table: table.to_owned(),
            column_positions: None,
            referenced_columns: HashSet::new(),
            references: HashMap::new(),
            tracked_columns,
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

    // pub fn link_dependencies(&mut self, per_table: &HashMap<String, Rc<RefCell<RowCheck>>>) {
    //     for dep in self.get_column_dependencies() {
    //         self.add_dependency(Rc::<RefCell<RowCheck>>::downgrade(&per_table[&dep.table]));
    //     }
    // }

    pub fn test(&mut self, pass: &usize, insert_statement: &str, lookup_table: &HashMap<String, HashSet<String>>) -> bool {
        self.fulfill_dependency(pass);

        self.resolve_column_positions(insert_statement);

        let values = get_values(insert_statement);
        let value_per_field = self.pick_values(&self.tracked_columns, &values);

        let all_checks_passed = self.checks.iter().all(|t| {
            t.test(value_per_field[t.get_column_key()], lookup_table)
        });

        if all_checks_passed {
            self.capture_references(&values);
        }

        all_checks_passed
    }
}

fn new_col_test(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<Rc<RefCell<dyn ColumnTest>>, anyhow::Error> {
    let item: Rc<RefCell<dyn ColumnTest>> = if definition.contains("->") {
        Rc::new(RefCell::new(LookupTest::new(definition, table, data_types)?))
    } else {
        Rc::new(RefCell::new(CelTest::new(definition, table, data_types)?))
    };
    Ok(item)
}

fn new_test(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<Box<dyn ColumnTest>, anyhow::Error> {
    let item: Box<dyn ColumnTest> = if definition.contains("->") {
        Box::new(LookupTest::new(definition, table, data_types)?)
    } else {
        Box::new(CelTest::new(definition, table, data_types)?)
    };
    Ok(item)
}

fn parse_checks<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
    conditions: I,
    data_types: &HashMap<String, sqlparser::ast::DataType>,
) -> Result<HashMap<String, Vec<Box<dyn ColumnTest>>>, anyhow::Error> {
    let mut all_checks: Vec<Box<dyn ColumnTest>> = Vec::new();
    for check in conditions.flat_map(|(table, conditions)| conditions.iter().map(|c| new_test(table, c, data_types))) {
        all_checks.push(check?);
    }
    Ok(all_checks.into_iter().into_group_map_by(|x| x.get_table_name().to_owned()))
}


fn determine_columns<'a>(
    grouped_cols: &'a HashMap<String, HashSet<ColumnMeta>>,
    grouped_referenced_cols: &'a HashMap<String, HashSet<ColumnMeta>>,
    grouped_checks: &'a HashMap<String, Vec<Box<dyn ColumnTest>>>,
) -> Result<HashMap<String, Rc<RefCell<RowCheck<'a>>>>, anyhow::Error> {
    let mut res: HashMap<String, Rc<RefCell<RowCheck<'a>>>> = HashMap::new();
    for (table, tracked_columns) in grouped_cols {
        let checks = grouped_checks.get(table).ok_or(anyhow::anyhow!("Grouped checks don't have table: {}", table))?;
        res.insert(table.to_owned(), Rc::new(RefCell::new(RowCheck::from_config(&table, &tracked_columns, checks)?)));
    }
    Ok(res)
}


type Check = Rc<RefCell<dyn ColumnTest>>;
type Column = Rc<RefCell<ColumnMeta>>;

#[derive(Debug)]
pub struct CheckCollection {
    checks: Vec<Check>,
    tracked_columns: HashMap<String, Vec<Column>>,
}

impl CheckCollection {
    fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<Self, anyhow::Error> {
        let mut checks: Vec<Rc<RefCell<dyn ColumnTest>>> = Vec::new();
        for check in conditions.flat_map(|(table, conditions)| conditions.iter().map(|c| new_col_test(table, c, data_types))) {
            checks.push(check?);
        }

        let mut tracked_columns: HashMap<String, Vec<Column>> = checks.iter().flat_map(|c| {
            c.borrow().get_tracked_columns().iter().map(|c| Rc::new(RefCell::new(c.to_owned()))).collect::<Vec<Rc<RefCell<ColumnMeta>>>>()
        }).into_group_map_by(|x| x.borrow().get_table_name().to_owned());

        Ok(CheckCollection {
            checks,
            tracked_columns,
        })
    }
}

pub fn from_config<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
    conditions: I,
    data_types: &HashMap<String, sqlparser::ast::DataType>,
) -> Result<HashMap<String, Rc<RefCell<RowCheck>>>, anyhow::Error> {
    let collection = CheckCollection::new(conditions, &data_types);

    dbg!(&collection);

    panic!("stop");
    let grouped_checks = parse_checks(conditions, data_types)?;

    let mut tracked_columns: Vec<ColumnMeta> = grouped_checks.values().flat_map(|c| {
        c.iter().flat_map(|x| x.get_tracked_columns().into_iter())
    }).collect();

    tracked_columns.dedup_by_key(|c| c.get_column_key().to_owned());
    let grouped_cols: HashMap<String, HashSet<ColumnMeta>> = tracked_columns.into_iter().into_group_map_by(|x| x.get_table_name().to_owned())
      .iter()
      .map(|(t, v)| (t.to_owned(), v.into_iter().map(|x| x.to_owned()).collect::<HashSet<ColumnMeta>>())).collect();

    let mut referenced_columns: Vec<ColumnMeta> = grouped_checks.values().flat_map(|c| {
        c.iter().flat_map(|x| x.get_column_dependencies().into_iter())
    }).collect();
    referenced_columns.dedup_by_key(|c| c.get_column_key().to_owned());
    let grouped_referenced_cols: HashMap<String, HashSet<ColumnMeta>> = referenced_columns.into_iter().into_group_map_by(|x| x.get_table_name().to_owned())
      .iter()
      .map(|(t, v)| (t.to_owned(), v.into_iter().map(|x| x.to_owned()).collect::<HashSet<ColumnMeta>>())).collect();

    let mut result = determine_columns(&grouped_cols, &grouped_referenced_cols, &grouped_checks)?;

    dbg!(result);

    dbg!(&grouped_referenced_cols);


    Ok(result)
}

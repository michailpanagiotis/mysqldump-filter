use cel_interpreter::{Context, Program};
use cel_interpreter::extractors::This;
use chrono::NaiveDateTime;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone)]
#[derive(Debug)]
pub struct ColumnMeta {
    key: String,
    table: String,
    column: String,
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
    fn get_definition(&self) -> &str;
    fn get_column_meta(&self) -> &ColumnMeta;
    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta;
    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool;

    fn get_column_key(&self) -> &str {
        &self.get_column_meta().key
    }

    fn get_table_name(&self) -> &str {
        &self.get_column_meta().table
    }

    fn get_column_name(&self) -> &str {
        &self.get_column_meta().column
    }

    fn get_data_type(&self) -> &sqlparser::ast::DataType {
        &self.get_column_meta().data_type
    }

    fn get_column_position(&self) -> &Option<usize> {
        &self.get_column_meta().position
    }

    fn get_table_dependencies(&self) -> HashSet<String> {
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

    fn extend_allowed_values(&mut self) {

    }
}

#[derive(Debug)]
pub struct CelTest {
    column_meta: ColumnMeta,
    definition: String,
    program: Program,
}

impl Clone for CelTest
{
    fn clone(&self) -> Self {
        let data_types = HashMap::from([(self.get_column_name().to_owned(), self.get_data_type().to_owned())]);
        CelTest::from_definition(self.get_table_name(), self.get_definition(), &data_types)
    }
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
            _ => panic!("{}", format!("cannot parse {} for {}", other_value, data_type))
        };

        context
    }
}

impl TestValue for CelTest {
    fn get_definition(&self) -> &str {
        &self.definition
    }

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
            definition: definition.to_owned(),
            program,
        }
    }
}

#[derive(Debug)]
pub struct LookupTest {
    column_meta: ColumnMeta,
    definition: String,
    lookup_key: String,
    target_table: String,
    target_column: String,
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
            definition: definition.to_owned(),
            lookup_key: foreign_key.to_string(),
            target_table: target_table.to_string(),
            target_column: target_column.to_string(),
        }
    }

    pub fn get_foreign_key(&self) -> (String, String) {
        (self.target_table.clone(), self.target_column.clone())
    }

    pub fn get_target_table(&self) -> String {
        self.target_table.clone()
    }
}

impl TestValue for LookupTest {
    fn get_definition(&self) -> &str {
        &self.definition
    }

    fn get_column_meta(&self) -> &ColumnMeta {
        &self.column_meta
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        &mut self.column_meta
    }

    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        let Some(fvs) = lookup_table else { return true };
        let Some(set) = fvs.get(self.lookup_key.as_str()) else { return false };
        set.contains(value)
    }

    fn get_table_dependencies(&self) -> HashSet<String> {
        HashSet::from([self.get_target_table()])
    }
}

#[derive(Debug)]
pub enum ValueTest {
    Lookup(LookupTest),
    Cel(CelTest),
}

impl ValueTest {
    fn from_definition(table: &str, condition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        if condition.contains("->") {
            ValueTest::Lookup(LookupTest::from_definition(condition, table, data_types))
        } else {
            ValueTest::Cel(CelTest::from_definition(condition, table, data_types))
        }
    }
}

impl TestValue for ValueTest {
    fn get_definition(&self) -> &str {
        match &self {
            ValueTest::Cel(cond) => cond.get_definition(),
            ValueTest::Lookup(cond) => cond.get_definition(),
        }
    }

    fn test(&self, value: &str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        match &self {
            ValueTest::Cel(cond) => cond.test(value, lookup_table),
            ValueTest::Lookup(cond) => cond.test(value, lookup_table),
        }
    }

    fn get_column_meta(&self) -> &ColumnMeta {
        match &self {
            ValueTest::Lookup(t) => t.get_column_meta(),
            ValueTest::Cel(t) => t.get_column_meta()
        }
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        match self {
            ValueTest::Lookup(t) => t.get_column_meta_mut(),
            ValueTest::Cel(t) => t.get_column_meta_mut()
        }
    }
}


impl core::fmt::Debug for dyn TestValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.get_column_meta().fmt(f)
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct RowCheck {
    checks: Vec<Box<dyn TestValue>>,
}

impl RowCheck {
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

    pub fn get_table_dependencies(&self) -> HashSet<String> {
        let mut dependencies = HashSet::new();
        for condition in self.checks.iter() {
            for dependency in condition.get_table_dependencies() {
                dependencies.insert(dependency);
            }
        }
        dependencies
    }

    pub fn test(&self, values: &[&str], lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        self.checks.iter().all(|t| t.test_row(values, lookup_table))
    }
}

impl Extend<ValueTest> for RowCheck {
    fn extend<T: IntoIterator<Item=ValueTest>>(&mut self, iter: T) {
        for elem in iter {
            self.checks.push(Box::new(elem));
        }
    }
}

pub fn from_config(filters: &HashMap<String, Vec<String>>, cascades: &HashMap<String, Vec<String>>, data_types: &HashMap<String, sqlparser::ast::DataType>) -> (Vec<ValueTest>, HashMap<String, RowCheck>) {
    let mut collected1: Vec<ValueTest> = filters.iter().chain(cascades)
        .flat_map(|(table, conditions)| conditions.iter().map(move |condition| {
            ValueTest::from_definition(table, condition, data_types)
        }))
        .collect();
    let mut collected2: Vec<ValueTest> = filters.iter().chain(cascades)
        .flat_map(|(table, conditions)| conditions.iter().map(move |condition| {
            ValueTest::from_definition(table, condition, data_types)
        }))
        .collect();
    collected1.sort_by_key(|x| x.get_table_name().to_owned());
    collected2.sort_by_key(|x| x.get_table_name().to_owned());
    // let per_table: HashMap<String, Vec<&ValueTest>> = collected.iter().into_group_map_by(|x| x.get_table_name().to_owned());
    let per_table: HashMap<String, RowCheck> = collected1.into_iter().into_grouping_map_by(|x| x.get_table_name().to_owned()).collect();
    (collected2, per_table)
}

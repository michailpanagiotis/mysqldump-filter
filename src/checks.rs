use cel_interpreter::{Context, Program};
use cel_interpreter::extractors::This;
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug)]
struct ColumnMeta {
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
    fn from_definition(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self;
    fn get_column_meta(&self) -> &ColumnMeta;
    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta;
    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool;
    fn as_column_test(self) -> ColumnTest;

    fn get_table_name(&self) -> &str {
        &self.get_column_meta().table
    }

    fn get_column_name(&self) -> &str {
        &self.get_column_meta().column
    }

    fn get_column_position(&self) -> &Option<usize> {
        &self.get_column_meta().position
    }

    fn has_resolved_position(&self) -> bool {
        self.get_column_meta().position.is_some()
    }

    fn set_position(&mut self, pos: usize) {
        self.get_column_meta_mut().set_position(pos);
    }

    fn extend_allowed_values(&mut self) {

    }
}

#[derive(Debug)]
pub struct CelTest {
    meta: ColumnMeta,
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

        if other_value == "NULL" {
            context.add_variable(self.meta.column.clone(), false).unwrap();
            return context;
        }

        match self.meta.data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                context.add_variable(&self.meta.column, parse_int(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                context.add_variable(&self.meta.column, parse_date(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Enum(_, _) => {
                context.add_variable(&self.meta.column, other_value).unwrap();
            },
            _ => panic!("{}", format!("cannot parse {} for {}", other_value, self.meta.data_type))
        };

        context
    }
}

impl TestValue for CelTest {
    fn from_definition(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];

        CelTest {
            meta: ColumnMeta::new(table, column, data_types),
            program,
        }
    }

    fn get_column_meta(&self) -> &ColumnMeta {
        &self.meta
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        &mut self.meta
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

    fn as_column_test(self) -> ColumnTest {
        ColumnTest {
            test: ValueTest::Cel(self),
        }
    }
}

#[derive(Debug)]
pub struct LookupTest {
    meta: ColumnMeta,
    table: String,
    column: String,
    lookup_key: String,
    target_table: String,
    target_column: String,
}

impl LookupTest {
    pub fn get_foreign_key(&self) -> (String, String) {
        (self.target_table.clone(), self.target_column.clone())
    }

    pub fn get_target_table(&self) -> String {
        self.target_table.clone()
    }

    pub fn get_key(&self) -> String {
        self.target_table.clone() + "." + &self.target_column
    }
}

impl TestValue for LookupTest {
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
            meta: ColumnMeta::new(table, source_column, data_types),
            table: table.to_owned(),
            column: source_column.to_owned(),
            lookup_key: foreign_key.to_string(),
            target_table: target_table.to_string(),
            target_column: target_column.to_string(),
        }
    }

    fn get_column_meta(&self) -> &ColumnMeta {
        &self.meta
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        &mut self.meta
    }

    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        let Some(fvs) = lookup_table else { return true };
        let Some(set) = fvs.get(self.lookup_key.as_str()) else { return false };
        set.contains(value)
    }

    fn as_column_test(self) -> ColumnTest {
        ColumnTest {
            test: ValueTest::Cascade(self),
        }
    }
}

#[derive(Debug)]
pub enum ValueTest {
    Cascade(LookupTest),
    Cel(CelTest),
}

#[derive(Debug)]
pub struct ColumnTest {
    pub test: ValueTest,
}

impl TestValue for ColumnTest {
    fn from_definition(table: &str, condition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> ColumnTest {
        if condition.contains("->") {
            LookupTest::from_definition(condition, table, data_types).as_column_test()
        } else {
            CelTest::from_definition(condition, table, data_types).as_column_test()
        }
    }

    fn test(&self, value: &str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        match &self.test {
            ValueTest::Cel(cond) => cond.test(value, lookup_table),
            ValueTest::Cascade(cond) => cond.test(value, lookup_table),
        }
    }

    fn get_column_meta(&self) -> &ColumnMeta {
        match &self.test {
            ValueTest::Cascade(t) => &t.meta,
            ValueTest::Cel(t) => &t.meta
        }
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        match self.test {
            ValueTest::Cascade(ref mut t) => &mut t.meta,
            ValueTest::Cel(ref mut t) => &mut t.meta
        }
    }

    fn as_column_test(self) -> ColumnTest {
        self
    }
}

pub fn from_config(filters: &HashMap<String, Vec<String>>, cascades: &HashMap<String, Vec<String>>, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Vec<ColumnTest> {
    let mut collected: Vec<ColumnTest> = filters.iter().chain(cascades)
        .flat_map(|(table, conditions)| conditions.iter().map(move |condition| {
            ColumnTest::from_definition(table, condition, data_types)
        }))
        .collect();
    collected.sort_by_key(|x| x.get_table_name().to_owned());
    collected
}

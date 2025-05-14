use cel_interpreter::{Context, Program};
use cel_interpreter::extractors::This;
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub trait AllowValue {
    fn get_column_name_from_definition(definition: &str) -> String;
    fn from_definition(definition: &str, data_type: sqlparser::ast::DataType) -> Self;
    fn get_column_name(&self) -> &str;
    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool;
    fn extend_allowed_values(&mut self);
}

#[derive(Debug)]
pub struct CelTest {
    column: String,
    program: Program,
    data_type: sqlparser::ast::DataType,
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
            context.add_variable(self.column.clone(), false).unwrap();
            return context;
        }

        match self.data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                context.add_variable(&self.column, parse_int(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                context.add_variable(&self.column, parse_date(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Enum(_, _) => {
                context.add_variable(&self.column, other_value).unwrap();
            },
            _ => panic!("{}", format!("cannot parse {} for {}", other_value, self.data_type))
        };

        context
    }
}

impl AllowValue for CelTest {
    fn get_column_name_from_definition(definition: &str) -> String {
        let program = Program::compile(definition).expect("cannot compile CEL");
        program.references().variables().iter().map(|f| f.to_string()).next().expect("cannot find variable")
    }

    fn from_definition(definition: &str, data_type: sqlparser::ast::DataType) -> Self {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];

        CelTest {
            column: column.to_string(),
            program,
            data_type,
        }
    }

    fn get_column_name(&self) -> &str {
        &self.column
    }

    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        let context = self.build_context(value);
        match self.program.execute(&context).unwrap() {
            cel_interpreter::objects::Value::Bool(v) => {
                // println!("testing {}.{} {} -> {}", self.table, self.column, &other_value, &v);
                v
            }
            _ => panic!("filter does not return a boolean"),
        }
    }

    fn extend_allowed_values(&mut self) {

    }
}

#[derive(Debug)]
pub struct LookupTest {
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

impl AllowValue for LookupTest {
    fn get_column_name_from_definition(definition: &str) -> String {
        let mut split = definition.split("->");
        let Some(variable) = split.next() else {
            panic!("cannot parse cascade");
        };
        variable.to_string()
    }

    fn from_definition(definition: &str, data_type: sqlparser::ast::DataType) -> Self {
        let mut split = definition.split("->");
        let (Some(_), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };

        let mut split = foreign_key.split('.');
        let (Some(table), Some(column), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {foreign_key}");
        };

        LookupTest {
            column: LookupTest::get_column_name_from_definition(definition),
            lookup_key: foreign_key.to_string(),
            target_table: table.to_string(),
            target_column: column.to_string(),
        }
    }

    fn get_column_name(&self) -> &str {
        &self.column
    }

    fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        let Some(fvs) = lookup_table else { return true };
        let Some(set) = fvs.get(self.lookup_key.as_str()) else { return false };
        set.contains(value)
    }

    fn extend_allowed_values(&mut self) {

    }
}

#[derive(Debug)]
pub enum ValueTest {
    Cascade(LookupTest),
    Cel(CelTest),
}

#[derive(Debug)]
pub struct ColumnTest {
    pub table: String,
    pub column: String,
    pub test: ValueTest,
    pub position: Option<usize>,
}

impl ColumnTest {
    // pub resolve_inner_test() -> ValueTest {
    //
    // }

    pub fn new_cel(table: &str, condition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        let column = CelTest::get_column_name_from_definition(condition);
        let data_type = match data_types.get(&(table.to_owned() + "." + &column)) {
            None => panic!("{}", format!("cannot find data type for {table}.{column}")),
            Some(data_type) => data_type.to_owned()
        };

        let condition = CelTest::from_definition(condition, data_type);
        ColumnTest {
            table: table.to_owned(),
            column,
            test: ValueTest::Cel(condition),
            position: None,
        }
    }

    pub fn new_lookup(table: &str, condition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        let column = LookupTest::get_column_name_from_definition(condition);
        let data_type = match data_types.get(&(table.to_owned() + "." + &column)) {
            None => panic!("{}", format!("cannot find data type for {table}.{column}")),
            Some(data_type) => data_type.to_owned()
        };
        let condition = LookupTest::from_definition(condition, data_type);
        ColumnTest {
            table: table.to_owned(),
            column,
            test: ValueTest::Cascade(condition),
            position: None,
        }
    }

    pub fn from_config(filters: &HashMap<String, Vec<String>>, cascades: &HashMap<String, Vec<String>>, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Vec<ColumnTest> {
        let filter_iter = filters.iter()
            .flat_map(|(table, conditions)| conditions.iter().map(move |condition| {
                ColumnTest::new_cel(table, condition, data_types)
            }));
        let cascade_iter = cascades.iter()
            .flat_map(|(table, conditions)| conditions.iter().map(|condition| {
                ColumnTest::new_lookup(table, condition, data_types)
            }));

        let mut collected: Vec<ColumnTest> = filter_iter.chain(cascade_iter).collect();
        collected.sort_by_key(|x| x.table.clone());
        collected
    }


    pub fn test(&self, value: &str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        match &self.test {
            ValueTest::Cel(cond) => cond.test(value, lookup_table),
            ValueTest::Cascade(cond) => cond.test(value, lookup_table),
        }
    }

    pub fn get_column(&self) -> &str {
        &self.column
    }

    pub fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }
}

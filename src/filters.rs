use cel_interpreter::{Context, Program};
use cel_interpreter::extractors::This;
use chrono::NaiveDateTime;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::sql::{get_field_positions, get_values};
use crate::references::References;

#[derive(Debug)]
pub struct CelTest {
    field: String,
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
    fn resolve_field(definition: &str) -> String {
        let program = Program::compile(definition).expect("cannot compile CEL");
        program.references().variables().iter().map(|f| f.to_string()).next().expect("cannot find variable")
    }

    fn new(definition: &str, data_type: sqlparser::ast::DataType) -> Self {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let field = &variables[0];

        CelTest {
            field: field.to_string(),
            program,
            data_type,
        }
    }

    fn build_context(&self, other_value: &str) -> Context {
        let mut context = Context::default();
        context.add_function("timestamp", timestamp);

        if other_value == "NULL" {
            context.add_variable(self.field.clone(), false).unwrap();
            return context;
        }

        match self.data_type {
            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::Int(_) => {
                context.add_variable(&self.field, parse_int(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                context.add_variable(&self.field, parse_date(other_value)).unwrap();
            },
            sqlparser::ast::DataType::Enum(_, _) => {
                context.add_variable(&self.field, other_value).unwrap();
            },
            _ => panic!("{}", format!("cannot parse {} for {}", other_value, self.data_type))
        };

        context
    }

    pub fn test(&self, other_value: &str) -> bool {
        let context = self.build_context(other_value);
        match self.program.execute(&context).unwrap() {
            cel_interpreter::objects::Value::Bool(v) => {
                // println!("testing {}.{} {} -> {}", self.table, self.field, &other_value, &v);
                v
            }
            _ => panic!("filter does not return a boolean"),
        }
    }
}

#[derive(Debug)]
pub struct LookupTest {
    lookup_key: String,
    target_table: String,
    target_column: String,
}

impl LookupTest {
    fn resolve_field(definition: &str) -> String {
        let mut split = definition.split("->");
        let Some(field) = split.next() else {
            panic!("cannot parse cascade");
        };
        field.to_string()
    }

    pub fn new(definition: &str) -> Self {
        let mut split = definition.split("->");
        let (Some(_), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };

        let mut split = foreign_key.split('.');
        let (Some(table), Some(field), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {foreign_key}");
        };

        LookupTest {
            lookup_key: foreign_key.to_string(),
            target_table: table.to_string(),
            target_column: field.to_string(),
        }
    }

    pub fn test(&self, value:&str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        let Some(fvs) = lookup_table else { return true };
        let Some(set) = fvs.get(self.lookup_key.as_str()) else { return false };
        set.contains(value)
    }

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


#[derive(Debug)]
pub enum Tests {
    Cascade(LookupTest),
    Cel(CelTest),
}

#[derive(Debug)]
pub struct FieldCondition {
    pub table: String,
    pub field: String,
    pub test: Tests,
    pub position: Option<usize>,
    pub used_during_pass: Option<usize>,
}

impl FieldCondition {
    pub fn from_config(filters: &HashMap<String, Vec<String>>, cascades: &HashMap<String, Vec<String>>, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Vec<FieldCondition> {
        let filter_iter = filters.iter()
            .flat_map(|(table, conditions)| conditions.iter().map(move |c| {
                let field = CelTest::resolve_field(c);
                let data_type = match data_types.get(&(table.to_owned() + "." + &field)) {
                    None => panic!("{}", format!("cannot find data type for {table}.{field}")),
                    Some(data_type) => data_type.to_owned()
                };

                let condition = CelTest::new(c, data_type);
                FieldCondition {
                    table: table.clone(),
                    field,
                    test: Tests::Cel(condition),
                    position: None,
                    used_during_pass: None,
                }
            }));
        let cascade_iter = cascades.iter()
            .flat_map(|(table, conditions)| conditions.iter().map(|c| {
                let field = LookupTest::resolve_field(c);
                let condition = LookupTest::new(c);
                FieldCondition {
                    table: table.clone(),
                    field,
                    test: Tests::Cascade(condition),
                    position: None,
                    used_during_pass: None,
                }
            }));

        filter_iter.chain(cascade_iter).collect()
    }

    pub fn test(&self, value: &str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        match &self.test {
            Tests::Cel(cond) => cond.test(value),
            Tests::Cascade(cond) => cond.test(value, lookup_table),
        }
    }

    pub fn get_field(&self) -> &str {
        &self.field
    }

    pub fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }
}

#[derive(Debug)]
pub struct FilterConditions {
    pub inner: HashMap<String, HashMap<String, Vec<FieldCondition>>>,
    all_filtered_tables: HashSet<String>,
    pub pending_tables: HashSet<String>,
    pub fully_filtered_tables: HashMap<String, usize>,
    pub current_pass: usize,
}

impl FilterConditions {
    pub fn new(filters: &HashMap<String, Vec<String>>, cascades: &HashMap<String, Vec<String>>, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Self {
        let filter_iter = filters.iter()
            .flat_map(|(table, conditions)| conditions.iter().map(move |c| {
                let field = CelTest::resolve_field(c);
                let data_type = match data_types.get(&(table.to_owned() + "." + &field)) {
                    None => panic!("{}", format!("cannot find data type for {table}.{field}")),
                    Some(data_type) => data_type.to_owned()
                };

                let condition = CelTest::new(c, data_type);
                FieldCondition {
                    table: table.clone(),
                    field,
                    test: Tests::Cel(condition),
                    position: None,
                    used_during_pass: None,
                }
            }));
        let cascade_iter = cascades.iter()
            .flat_map(|(table, conditions)| conditions.iter().map(|c| {
                let field = LookupTest::resolve_field(c);
                let condition = LookupTest::new(c);
                FieldCondition {
                    table: table.clone(),
                    field,
                    test: Tests::Cascade(condition),
                    position: None,
                    used_during_pass: None,
                }
            }));

        let mut collected: Vec<FieldCondition> = filter_iter.chain(cascade_iter).collect();
        collected.sort_by_key(|x| x.table.clone());
        FilterConditions {
            inner: collected.into_iter()
                .chunk_by(|x| x.table.to_string())
                .into_iter()
                .map(|(table, conds)| (table, conds.into_iter().into_group_map_by(|x| x.field.to_string()))).collect(),
            all_filtered_tables: HashSet::new(),
            pending_tables: HashSet::new(),
            fully_filtered_tables: HashMap::new(),
            current_pass: 0,
        }
    }

    fn has_resolved_positions(&self, table: &str) -> bool {
        self.inner[table].values().flatten().all(|condition| {
            condition.position.is_some()
        })
    }

    fn resolve_positions(&mut self, table: &str, insert_statement: &str) {
        let positions: HashMap<String, usize> = get_field_positions(insert_statement);
        for condition in self.inner.get_mut(table).expect("unknown table").values_mut().flatten() {
            match positions.get(condition.get_field()) {
                Some(pos) => condition.set_position(*pos),
                None => panic!("{}", format!("unknown column {}", condition.get_field())),
            }
        }
        assert!(self.has_resolved_positions(table));
    }

    pub fn get_table_dependencies(&self, table: &str) -> HashSet<String> {
        let mut dependencies = HashSet::new();
        if !self.inner.contains_key(table) || self.inner[table].is_empty() {
            return dependencies;
        }

        for condition in self.inner[table].values().flatten() {
            if let Tests::Cascade(ref t) = condition.test {
                dependencies.insert(t.get_target_table());
            }
        }
        dependencies
    }

    pub fn can_table_be_fully_filtered(&self, table: &str, lookup_table: &Option<HashMap<String, HashSet<String>>>) -> bool {
        if !self.inner.contains_key(table) || self.inner[table].is_empty() {
            return false;
        }
        for condition in self.inner[table].values().flatten() {
            if let Tests::Cascade(ref t) = condition.test {
                let Some(l) = lookup_table else {
                    return false;
                };
                if !l.contains_key(&t.get_key()) {
                    return false;
                }
            }
        }
        return true;
    }

    pub fn track_filtered(&mut self, table: &str) {
        if !self.fully_filtered_tables.contains_key(table) {
            let dependencies = self.get_table_dependencies(table);
            for dependency in &dependencies {
                if !self.fully_filtered_tables.contains_key(dependency) {
                    self.pending_tables.insert(table.to_owned());
                    return;
                }
            }

            self.pending_tables.remove(table);

            let last_pass: Option<usize> = dependencies.iter().map(|d| self.fully_filtered_tables[d]).max();
            self.fully_filtered_tables.insert(table.to_owned(), self.current_pass);
        }
    }

    pub fn test_sql_statement(
        &mut self,
        sql_statement: &str,
        table: &str,
        lookup_table: &Option<HashMap<String, HashSet<String>>>,
    ) -> bool {
        if !sql_statement.starts_with("INSERT") {
            return true;
        }

        if self.fully_filtered_tables.get(table).is_some_and(|x| x < &self.current_pass) {
            return true;
        }

        self.track_filtered(table);

        if !self.inner.contains_key(table) || self.inner[table].is_empty() {
            return true;
        }

        if !self.has_resolved_positions(table) {
            self.resolve_positions(table, sql_statement);
        }

        let values = get_values(sql_statement);

        if !self.inner[table].values().flatten().all(|condition| {
            condition.position.is_some_and(|p| condition.test(values[p], lookup_table))
        }) {
            return false;
        }

        true
    }

    pub fn filter<I: Iterator<Item=(Option<String>, String)>>(&mut self, statements: I, references: &mut References) -> impl Iterator<Item=(Option<String>, String)> {
        self.current_pass += 1;
        let lookup = if references.is_empty() { None } else {
            let lookup = references.get_lookup_table();
            dbg!(&self.fully_filtered_tables);
            dbg!(&self.pending_tables);
            references.clear();
            Some(lookup)
        };
        statements.filter(move |(table_option, statement)| {
            let Some(table) = table_option else { return true };
            let should_keep = self.test_sql_statement(statement, table, &lookup);
            if should_keep {
                references.capture(table, statement);
            }
            should_keep
        })
    }
}

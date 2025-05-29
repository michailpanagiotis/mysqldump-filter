use cel_interpreter::{Context, Program};
use chrono::NaiveDateTime;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::traits::{ColumnMeta, ColumnPositions, ReferenceTracker, Dependency, PlainColumnCheck};
use crate::sql::{get_values, read_table_data_file};

type PlainCheckType = Box<dyn PlainColumnCheck>;
type ColumnType = ColumnMeta;
pub type TrackedColumnType = ColumnMeta;
type DependencyType = Weak<RefCell<dyn Dependency>>;

fn new_plain_test(table: &str, definition: &str) -> Result<PlainCheckType, anyhow::Error> {
    let item: PlainCheckType = if definition.contains("->") {
        Box::new(PlainLookupTest::new(definition, table)?)
    } else {
        Box::new(PlainCelTest::new(definition, table)?)
    };
    Ok(item)
}

impl From<&ColumnMeta> for ColumnType {
    fn from(c: &ColumnMeta) -> Self {
        c.to_owned()
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
    fn resolve_column_meta(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<(ColumnMeta, Vec<String>), anyhow::Error> {
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
    fn resolve_column_meta(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<(ColumnMeta, Vec<String>), anyhow::Error> {
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

#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    pub columns: HashMap<String, ColumnMeta>,
    // trait ColumnPositions
    column_positions: Option<HashMap<String, usize>>,
    // trait ReferenceTracker
    references: HashMap<String, HashSet<String>>,
    checks: Vec<Box<dyn PlainColumnCheck>>,
    dependencies: Vec<DependencyType>,
}

impl ColumnPositions for TableMeta {
    fn get_column_positions(&self) -> &Option<HashMap<String, usize>> {
        &self.column_positions
    }

    fn set_column_positions(&mut self, positions: HashMap<String, usize>) {
        self.column_positions = Some(positions.to_owned());
        for col in self.columns.values_mut() {
            col.capture_position(&positions);
        }
    }
}

impl ReferenceTracker for TableMeta {
    fn get_referenced_columns(&self) -> impl Iterator<Item=&ColumnMeta> {
        self.columns.values().filter(|v| v.is_referenced())
    }

    fn get_references(&self) -> &HashMap<String, HashSet<String>> {
        &self.references
    }

    fn get_references_mut(&mut self) -> &mut HashMap<String, HashSet<String>> {
        &mut self.references
    }
}

impl Dependency for TableMeta {
    fn get_dependencies(&self) -> &[Weak<RefCell<dyn Dependency>>] {
        &self.dependencies
    }

    fn set_fulfilled_at_depth(&mut self, depth: &usize) {
        self.columns.values_mut().for_each(|v| v.set_fulfilled_at_depth(depth))
    }

    fn has_been_fulfilled(&self) -> bool {
        self.columns.values().all(|v| v.has_been_fulfilled())
    }
}

impl Extend<ColumnMeta> for Rc<RefCell<TableMeta>> {
    fn extend<T: IntoIterator<Item=ColumnMeta>>(&mut self, iter: T) {
        let mut borrowed_self = self.borrow_mut();
        for elem in iter {
            borrowed_self.add_column_meta(elem);
        }
    }
}

impl TableMeta {
    fn add_column_meta(&mut self, elem: ColumnMeta) {
        if self.table.is_empty() {
            self.table = elem.get_table_name().to_owned();
        } else if self.table != elem.get_table_name() {
            panic!("mismatched table names");
        }

        let key = elem.get_column_name().to_owned();

        for check in elem.get_checks() {
            self.checks.push(new_plain_test(&self.table, check).unwrap())
        }
        match self.columns.get_mut(&key) {
            None => {
                self.columns.insert(key.to_owned(), elem);
            },
            Some(cm) => {
                cm.extend(&elem);
            }
        }
        if self.columns[&key].is_referenced() {
            self.references.insert(self.columns[&key].get_column_key().to_owned(), HashSet::new());
        }
    }

    pub fn get_dependency_keys(&self) -> Vec<String> {
        self.columns.values().flat_map(|v| v.get_dependency_keys().map(|x| x.to_owned())).collect()
    }

    fn add_dependency(&mut self, target: &Rc<RefCell<TableMeta>>) {
        let weak = Rc::downgrade(target);
        self.dependencies.push(weak);
    }

    fn get_checks(&self) -> impl Iterator<Item=&Box<dyn PlainColumnCheck>> {
        self.checks.iter()
    }

    fn get_tracked_columns(&self) -> impl Iterator<Item=&ColumnMeta> {
        self.columns.values()
    }

    pub fn test(&mut self, pass: &usize, sql_statement: &str, lookup_table: &HashMap<String, HashSet<String>>) -> Result<bool, anyhow::Error> {
        if !sql_statement.starts_with("INSERT") {
            return Ok(true);
        }

        if !self.has_fulfilled_dependencies() {
            return Ok(true);
        }

        self.fulfill_dependency(pass);

        self.resolve_column_positions(sql_statement);

        let values = get_values(sql_statement);
        let value_per_field = self.pick_values(self.get_tracked_columns(), &values);

        let all_checks_passed = self.get_checks().all(|t| {
            let column_meta = &self.columns[t.get_column_name()];
            t.test(column_meta, value_per_field[column_meta.get_column_key()], lookup_table)
        });

        if all_checks_passed {
            self.capture_references(&values)?;
        }

        Ok(all_checks_passed)
    }

    pub fn process_data_file(&mut self, current_pass: &usize, file: &Path, lookup_table: &HashMap<String, HashSet<String>>) -> Result<(), anyhow::Error> {
        if !self.has_fulfilled_dependencies() {
            println!("Skipping table {} since it still has dependencies", &self.table);
            return Ok(());
        }
        println!("Processing table {}", self.table);
        let current_table = &self.table.clone();
        let table_file = file.to_path_buf();
        let input_file = &table_file.with_extension("proc");
        fs::rename(&table_file, input_file).expect("cannot rename");
        fs::File::create(&table_file)?;

        let mut writer = BufWriter::new(
            fs::OpenOptions::new()
            .append(true)
            .open(&table_file)?
        );

        let statements = read_table_data_file(current_table, input_file);
        for (table_option, sql_statement) in statements {
            let Some(ref table) = table_option else { return Err(anyhow::anyhow!("unknown table")) };
            if current_table != table {
                return Err(anyhow::anyhow!("wrong table {} != {}", current_table, table));
            }

            let passed = self.test(current_pass, &sql_statement, lookup_table)?;
            if passed {
                writer.write_all(sql_statement.as_bytes())?;
            }
        }

        writer.flush()?;

        Ok(())
    }
}

pub fn parse_test_definition(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<(ColumnMeta, Vec<String>), anyhow::Error> {
    let (mut column_meta, deps) = if definition.contains("->") {
        PlainLookupTest::resolve_column_meta(table, definition, data_types)?
    } else {
        PlainCelTest::resolve_column_meta(table, definition, data_types)?
    };
    column_meta.add_check(definition);
    Ok((column_meta, deps))
}

#[derive(Debug)]
pub struct CheckCollection {
    table_meta: HashMap<String, Rc<RefCell<TableMeta>>>,
}

impl CheckCollection {
    fn parse_columns<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<HashMap<String, Rc<RefCell<TableMeta>>>, anyhow::Error> {
        let definitions: Vec<(String, String)> = conditions.flat_map(|(table, conds)| {
            conds.iter().map(|c| (table.to_owned(), c.to_owned()))
        }).collect();

        let mut tracked_cols: Vec<TrackedColumnType> = Vec::new();
        let mut all_deps: HashMap<String, Vec<String>> = HashMap::new();

        for (table, definition) in definitions.iter() {
            let (column_meta, deps) = parse_test_definition(table, definition, data_types)?;
            let key = &column_meta.get_column_key().to_string();
            tracked_cols.push(column_meta);

            // track target columns
            if !deps.is_empty() {
                all_deps.insert(key.to_owned(), deps.clone());
            }
            for key in deps {
                let (target_table, target_column) = ColumnMeta::get_components_from_key(&key)?;
                let mut column_meta = ColumnMeta::new(&target_table, &target_column, &Vec::new(), data_types)?;
                column_meta.set_referenced();
                tracked_cols.push(column_meta);
            }
        }
        let grouped: HashMap<String, Rc<RefCell<TableMeta>>> = tracked_cols
            .into_iter()
            .into_grouping_map_by(|t| t.get_table_name().to_owned())
            .collect();

        for table_meta in grouped.values() {
            let mut table_borrow = table_meta.borrow_mut();
            let deps = table_borrow.get_dependency_keys();
            for dep in deps.iter() {
                let (target_table, _) = ColumnMeta::get_components_from_key(dep)?;
                let target_table_meta = &grouped[&target_table];
                table_borrow.add_dependency(target_table_meta);
            }
        }

        Ok(grouped)
    }

    pub fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<Self, anyhow::Error> {
        let grouped = CheckCollection::parse_columns(conditions, data_types)?;
        Ok(CheckCollection {
            table_meta: grouped,
        })
    }

    fn get_lookup_table(&self) -> HashMap<String, HashSet<String>> {
        let mut lookup_table: HashMap<String, HashSet<String>> = HashMap::new();

        for table_meta in self.table_meta.values() {
            lookup_table.extend(table_meta.borrow().get_references().iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
        }
        lookup_table
    }

    pub fn process_tables(&mut self, current_pass: &usize, table_files: &HashMap<String, PathBuf>) -> Result<(), anyhow::Error> {
        let lookup_table = self.get_lookup_table();
        for table_meta in self.table_meta.values_mut() {
            let file = table_files[&table_meta.borrow().table].to_path_buf();
            table_meta.borrow_mut().process_data_file(current_pass, &file, &lookup_table)?;
        }
        Ok(())
    }

    pub fn process(&mut self, current_pass: &usize, table_files: &HashMap<String, PathBuf>) -> Result<(), anyhow::Error> {
        self.process_tables(current_pass, table_files)?;

        Ok(())
    }
}

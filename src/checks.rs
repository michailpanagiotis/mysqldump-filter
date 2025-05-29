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

use crate::traits::{ColumnMeta, ColumnPositions, DBColumn, ColumnTest, ReferenceTracker, Dependency};
use crate::sql::{get_values, read_table_data_file};

type CheckType = Box<dyn ColumnTest>;
type ColumnType = ColumnMeta;
pub type RowType = Rc<RefCell<RowCheck>>;
pub type TrackedColumnType = ColumnMeta;
type DependencyType = Weak<RefCell<dyn Dependency>>;

fn new_check<C: ColumnTest + 'static>(test: C) -> CheckType {
    Box::new(test)
}

fn new_col_test(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<CheckType, anyhow::Error> {
    let item: CheckType = if definition.contains("->") {
        new_check(LookupTest::new(definition, table, data_types)?)
    } else {
        new_check(CelTest::new(definition, table, data_types)?)
    };
    Ok(item)
}

impl From<&ColumnMeta> for ColumnType {
    fn from(c: &ColumnMeta) -> Self {
        c.to_owned()
    }
}

impl From<RowCheck> for RowType {
    fn from(c: RowCheck) -> Self {
        Rc::new(RefCell::new(c))
    }
}


#[derive(Debug)]
pub struct CelTest {
    definition: String,
    column_meta: ColumnMeta,
    program: Program,
}

impl CelTest {
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
            CelTest::parse_date(&d)
        });

        let column_name = column_meta.get_column_name();
        let data_type = column_meta.get_data_type();

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

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        &mut self.column_meta
    }
}

impl ColumnTest for CelTest {
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, anyhow::Error> where Self: Sized {
        let program = Program::compile(definition).unwrap();
        let variables: Vec<String> = program.references().variables().iter().map(|f| f.to_string()).collect();
        let column = &variables[0];
        let column_meta = ColumnMeta::new(table, column, &Vec::new(), data_types)?;

        Ok(CelTest {
            definition: definition.to_owned(),
            column_meta,
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
}

#[derive(Debug)]
pub struct LookupTest {
    definition: String,
    column_meta: ColumnMeta,
}

impl LookupTest {
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

impl DBColumn for LookupTest {
    fn get_column_meta(&self) -> &ColumnMeta {
        &self.column_meta
    }

    fn get_column_meta_mut(&mut self) -> &mut ColumnMeta {
        &mut self.column_meta
    }
}

impl ColumnTest for LookupTest {
    fn new(definition: &str, table: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<impl ColumnTest + 'static, anyhow::Error> where Self: Sized {
        let mut split = definition.split("->");
        let (Some(source_column), Some(foreign_key), None) = (split.next(), split.next(), split.next()) else {
            panic!("cannot parse cascade");
        };

        let mut column_meta = ColumnMeta::new(table, source_column, &Vec::new(), data_types)?;

        let mut split = foreign_key.split('.');
        let (Some(target_table), Some(target_column), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {foreign_key}");
        };

        Ok(LookupTest {
            definition: definition.to_owned(),
            column_meta,
        })
    }

    fn test(&self, column_meta: &ColumnMeta, value:&str, lookup_table: &HashMap<String, HashSet<String>>) -> bool {
        let mut found = false;
        for key in column_meta.get_column_dependencies().map(|d| d.get_column_key()) {
            let Some(set) = lookup_table.get(key) else { return true };
            if set.contains(value) {
                found = true;
                break;
            }
        }
        found
    }

    fn get_definition(&self) -> &str {
        &self.definition
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub columns: HashMap<String, ColumnMeta>,
    // trait ColumnPositions
    column_positions: Option<HashMap<String, usize>>,
    // trait ReferenceTracker
    references: HashMap<String, HashSet<String>>,
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
    fn get_dependencies(&self) -> impl Iterator<Item=&ColumnMeta> {
        std::iter::empty()
    }

    fn set_fulfilled_at_depth(&mut self, depth: &usize) {
        self.columns.values_mut().for_each(|v| v.set_fulfilled_at_depth(depth))
    }

    fn has_been_fulfilled(&self) -> bool {
        self.columns.values().all(|v| v.has_been_fulfilled())
    }
}

impl Extend<ColumnMeta> for TableMeta {
    fn extend<T: IntoIterator<Item=ColumnMeta>>(&mut self, iter: T) {
        for elem in iter {
            let key = elem.get_column_name();
            match self.columns.get_mut(key) {
                None => {
                    self.columns.insert(key.to_owned(), elem);
                },
                Some(cm) => {
                    cm.extend(&elem);
                }
            }
        }
    }
}

impl TableMeta {
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
            let column_meta = t.get_column_meta();
            t.test(&column_meta, value_per_field[t.get_column_key()], lookup_table)
        });

        if all_checks_passed {
            self.capture_references(&values)?;
        }

        Ok(all_checks_passed)
    }
}

#[derive(Debug)]
pub struct RowCheck {
    pub table: String,
    // trait ColumnPositions
    column_positions: Option<HashMap<String, usize>>,
    // trait ReferenceTracker
    referenced_columns: Vec<ColumnMeta>,
    references: HashMap<String, HashSet<String>>,
    // trait Dependency
    tested_at_pass: Option<usize>,
    tracked_columns: Vec<ColumnMeta>,
    checks: Vec<Box<dyn ColumnTest>>,
}

impl ColumnPositions for RowCheck {
    fn get_column_positions(&self) -> &Option<HashMap<String, usize>> {
        &self.column_positions
    }

    fn set_column_positions(&mut self, positions: HashMap<String, usize>) {
        self.column_positions = Some(positions.to_owned());
        for col in self.tracked_columns.iter_mut() {
            col.capture_position(&positions);
        }
        for check in self.checks.iter_mut() {
            check.get_column_meta_mut().capture_position(&positions);
        }
    }
}

impl ReferenceTracker for RowCheck {
    fn get_referenced_columns(&self) -> impl Iterator<Item=&ColumnMeta> {
        self.referenced_columns.iter()
    }

    fn get_references(&self) -> &HashMap<String, HashSet<String>> {
        &self.references
    }

    fn get_references_mut(&mut self) -> &mut HashMap<String, HashSet<String>> {
        &mut self.references
    }
}

impl Dependency for RowCheck {
    fn get_dependencies(&self) -> impl Iterator<Item=&ColumnMeta> {
        std::iter::empty()
    }

    fn set_fulfilled_at_depth(&mut self, depth: &usize) {
        self.tested_at_pass = Some(depth.to_owned());
    }

    fn has_been_fulfilled(&self) -> bool {
        self.tested_at_pass.is_some()
    }
}

impl RowCheck {
    pub fn from_config(table: &str, tracked_columns: &Vec<ColumnMeta>, checks: &Vec<Box<dyn ColumnTest>>, referenced_columns: &Vec<ColumnMeta>) -> Result<RowCheck, anyhow::Error> {
        let mut curr_checks: Vec<Box<dyn ColumnTest>> = Vec::new();
        for c in checks {
            curr_checks.push(
                new_col_test(
                    c.get_table_name(),
                    c.get_definition(),
                    &HashMap::from_iter(c.get_column_meta().get_referenced_columns().map(|c| {
                        (c.get_column_key().to_owned(), c.get_data_type().to_owned())
                    })),
                )?
            );
        }
        Ok(RowCheck {
            table: table.to_owned(),
            column_positions: None,
            referenced_columns: referenced_columns.clone(),
            references: HashMap::from_iter(referenced_columns.iter().map(|c| (c.get_column_key().to_owned(), HashSet::new()))),
            tracked_columns: tracked_columns.clone(),
            checks: curr_checks,
            tested_at_pass: None,
        })
    }

    fn get_checks(&self) -> impl Iterator<Item=&Box<dyn ColumnTest>> {
        self.checks.iter()
    }

    fn get_tracked_columns(&self) -> impl Iterator<Item=&ColumnMeta> {
        self.tracked_columns.iter()
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
            let column_meta = t.get_column_meta();
            t.test(&column_meta, value_per_field[t.get_column_key()], lookup_table)
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
        LookupTest::resolve_column_meta(table, definition, data_types)?
    } else {
        CelTest::resolve_column_meta(table, definition, data_types)?
    };
    column_meta.add_check(definition);
    Ok((column_meta, deps))
}

#[derive(Debug)]
pub struct CheckCollection {
    per_table: HashMap<String, RowType>,
}

impl CheckCollection {
    fn parse_columns<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<HashMap<String, TableMeta>, anyhow::Error> {
        let definitions: Vec<(String, String)> = conditions.map(|(table, conds)| {
            conds.iter().map(|c| (table.to_owned(), c.to_owned()))
        }).flatten().collect();

        dbg!(&definitions);

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

        Ok(
            tracked_cols
                .into_iter()
                .into_grouping_map_by(|t| t.get_table_name().to_owned())
                .collect()
        )
    }

    fn determine_checks<'a, I: Iterator<Item=&'a ColumnMeta>>(
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<HashMap<String, Vec<CheckType>>, anyhow::Error> {
        let mut checks: HashMap<String, Vec<CheckType>> = HashMap::new();
        let parsed: Vec<_> = conditions
            .flat_map(|column_meta| column_meta.get_checks().map(|c| new_col_test(column_meta.get_table_name(), c, data_types)))
            .collect();
        for check_option in parsed.into_iter() {
            let check = check_option?;
            let table_name = check.as_ref().get_table_name().to_owned();
            if !checks.contains_key(&table_name) {
                checks.insert(table_name.to_owned(), Vec::new());
            }

            checks.get_mut(&table_name).ok_or(anyhow::anyhow!("Grouped checks don't have table: {}", table_name))?.push(check);
        }
        Ok(checks)
    }

    fn determine_tracked_columns<'a, I: Iterator<Item=&'a CheckType>>(checks: I) -> HashMap<String, Vec<ColumnType>> {
        let mut tracked_columns = checks.flat_map(|c| {
            c.get_column_meta().get_referenced_columns().map(ColumnType::from).collect::<Vec<ColumnType>>()
        }).into_group_map_by(|x| x.get_table_name().to_owned());
        tracked_columns.values_mut().for_each(|v| v.dedup());
        tracked_columns
    }

    fn determine_referenced_columns<'a, I: Iterator<Item=&'a CheckType>>(checks: I) -> HashMap<String, Vec<ColumnType>> {
        let mut referenced_columns: HashMap<String, Vec<ColumnType>> = checks.flat_map(|c| {
            c.get_column_meta().get_column_dependencies().map(ColumnType::from).collect::<Vec<ColumnType>>()
        }).into_group_map_by(|x| x.get_table_name().to_owned());
        referenced_columns.values_mut().for_each(|v| v.dedup());
        referenced_columns
    }

    fn determine_checks_per_table<'a, I: Iterator<Item=(&'a String, &'a Vec<ColumnType>)>>(
        tracked_columns: I,
        checks: &'a HashMap<String, Vec<CheckType>>,
        referenced_columns: &'a HashMap<String, Vec<ColumnType>>,
    ) -> Result<HashMap<String, RowType>, anyhow::Error> {
        let mut res: HashMap<String, RowType> = HashMap::new();
        for (table, tracked_columns) in tracked_columns {
            let checks = &checks[table];
            let referenced_columns = &referenced_columns[table];
            res.insert(table.to_owned(), RowType::from(RowCheck::from_config(table, tracked_columns, checks, referenced_columns)?));
        }
        Ok(res)
    }

    pub fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<Self, anyhow::Error> {
        let grouped = CheckCollection::parse_columns(conditions, data_types)?;

        dbg!(&grouped);
        panic!("stop");

        let iter = grouped.values().map(|per_field| per_field.columns.values()).flatten();

        let mut checks = CheckCollection::determine_checks(iter, data_types)?;
        let tracked_columns = CheckCollection::determine_tracked_columns(checks.values().flatten());
        let mut referenced_columns = CheckCollection::determine_referenced_columns(checks.values().flatten());

        for (table, _) in tracked_columns.iter() {
            if !referenced_columns.contains_key(table) {
                referenced_columns.insert(table.to_owned(), Vec::new());
            }
            if !checks.contains_key(table) {
                checks.insert(table.to_owned(), Vec::new());
            }
        }

        let per_table = CheckCollection::determine_checks_per_table(tracked_columns.iter(), &checks, &referenced_columns)?;

        Ok(CheckCollection {
            per_table,
        })
    }

    fn get_lookup_table(&self) -> HashMap<String, HashSet<String>> {
        let mut lookup_table: HashMap<String, HashSet<String>> = HashMap::new();

        for row_check in self.per_table.values() {
            lookup_table.extend(row_check.borrow().get_references().iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
        }
        lookup_table
    }

    pub fn process_tables(&mut self, current_pass: &usize, table_files: &HashMap<String, PathBuf>) -> Result<(), anyhow::Error> {
        let lookup_table = self.get_lookup_table();
        for row_check in self.per_table.values_mut() {
            let file = table_files[&row_check.borrow().table].to_path_buf();
            row_check.borrow_mut().process_data_file(current_pass, &file, &lookup_table)?;
        }
        Ok(())
    }

    pub fn process(&mut self, current_pass: &usize, table_files: &HashMap<String, PathBuf>) -> Result<(), anyhow::Error> {
        self.process_tables(current_pass, table_files)?;

        Ok(())
    }
}

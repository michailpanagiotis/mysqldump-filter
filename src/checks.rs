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
use crate::sql::{get_values, read_table_data_file, write_sql_file};

type CheckType = Box<dyn ColumnTest>;
type ColumnType = ColumnMeta;
type RowType<'a> = Rc<RefCell<RowCheck<'a>>>;
type DependencyType = Weak<RefCell<dyn Dependency>>;

fn new_check<C: ColumnTest + 'static>(test: C) -> CheckType {
    Box::new(test)
}

impl From<&ColumnMeta> for ColumnType {
    fn from(c: &ColumnMeta) -> Self {
        c.to_owned()
    }
}

impl<'a> From<RowCheck<'a>> for RowType<'a> {
    fn from(c: RowCheck<'a>) -> Self {
        Rc::new(RefCell::new(c))
    }
}

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
    file: PathBuf,
    // trait ColumnPositions
    column_positions: Option<HashMap<String, usize>>,
    // trait ReferenceTracker
    referenced_columns: &'a Vec<ColumnMeta>,
    references: HashMap<String, HashSet<String>>,
    // trait Dependency
    tested_at_pass: Option<usize>,
    pending_dependencies: Vec<DependencyType>,
    tracked_columns: &'a Vec<ColumnMeta>,
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
    fn get_referenced_columns(&self) -> &Vec<ColumnMeta> {
        self.referenced_columns
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

    fn get_dependencies(&self) -> &Vec<DependencyType> {
        &self.pending_dependencies
    }

}

impl<'a> RowCheck<'a> {
    pub fn from_config(table: &str, file: &Path, tracked_columns: &'a Vec<ColumnMeta>, checks: &'a Vec<Box<dyn ColumnTest>>, referenced_columns: &'a Vec<ColumnMeta>) -> Result<RowCheck<'a>, anyhow::Error> {
        Ok(RowCheck {
            table: table.to_owned(),
            file: file.to_owned(),
            column_positions: None,
            referenced_columns,
            references: HashMap::from_iter(referenced_columns.iter().map(|c| (c.get_column_key().to_owned(), HashSet::new()))),
            tracked_columns,
            checks,
            tested_at_pass: None,
            pending_dependencies: Vec::new(),
        })
    }

    pub fn test(&mut self, pass: &usize, insert_statement: &str, lookup_table: &HashMap<String, HashSet<String>>) -> Result<bool, anyhow::Error> {
        if !self.is_ready_to_be_tested() {
            return Ok(true);
        }

        self.fulfill_dependency(pass);

        self.resolve_column_positions(insert_statement);

        let values = get_values(insert_statement);
        let value_per_field = self.pick_values(self.tracked_columns.iter(), &values);

        let all_checks_passed = self.checks.iter().all(|t| {
            t.test(value_per_field[t.get_column_key()], lookup_table)
        });

        if all_checks_passed {
            self.capture_references(&values)?;
        }

        Ok(all_checks_passed)
    }

    pub fn process_data_file(&mut self, current_pass: &usize, lookup_table: &HashMap<String, HashSet<String>>) -> Result<(), anyhow::Error> {
        println!("Processing table {}", self.table);
        let current_table = &self.table.clone();
        let table_file = self.file.clone();
        let input_file = &table_file.with_extension("proc");
        fs::rename(&table_file, input_file).expect("cannot rename");
        let statements = read_table_data_file(current_table, input_file);

        let mut writer = BufWriter::new(
            fs::OpenOptions::new()
            .append(true)
            .open(&self.file)?
        );

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


fn new_col_test(table: &str, definition: &str, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Result<CheckType, anyhow::Error> {
    let item: CheckType = if definition.contains("->") {
        new_check(LookupTest::new(definition, table, data_types)?)
    } else {
        new_check(CelTest::new(definition, table, data_types)?)
    };
    Ok(item)
}

#[derive(Debug)]
pub struct CheckCollection {
    files: HashMap<String, PathBuf>,
    checks: HashMap<String, Vec<CheckType>>,
    tracked_columns: HashMap<String, Vec<ColumnType>>,
    referenced_columns: HashMap<String, Vec<ColumnType>>,
}

impl CheckCollection {
    fn determine_checks<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<HashMap<String, Vec<CheckType>>, anyhow::Error> {
        let mut checks: HashMap<String, Vec<CheckType>> = HashMap::new();
        let parsed: Vec<_> = conditions
            .flat_map(|(table, conditions)| conditions.iter().map(|c| new_col_test(table, c, data_types)))
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

    fn determine_tracked_columns(checks: &HashMap<String, Vec<CheckType>>) -> HashMap<String, Vec<ColumnType>> {
        let mut tracked_columns = checks.values().flatten().flat_map(|c| {
            c.get_tracked_columns().iter().map(ColumnType::from).collect::<Vec<ColumnType>>()
        }).into_group_map_by(|x| x.get_table_name().to_owned());
        tracked_columns.values_mut().for_each(|v| v.dedup());

        for check in checks.values().flatten().filter(|c| c.get_column_dependencies().len() > 0) {
            for dep in check.get_column_dependencies() {
                let found = tracked_columns.values().flatten().find(|x| x.get_column_meta() == &dep);
                dbg!(found);
            }
        }

        tracked_columns
    }

    fn determine_referenced_columns(checks: &HashMap<String, Vec<CheckType>>) -> HashMap<String, Vec<ColumnType>> {
        let mut referenced_columns: HashMap<String, Vec<ColumnType>> = checks.values().flatten().flat_map(|c| {
            c.get_column_dependencies().iter().map(ColumnType::from).collect::<Vec<ColumnType>>()
        }).into_group_map_by(|x| x.get_table_name().to_owned());
        referenced_columns.values_mut().for_each(|v| v.dedup());
        referenced_columns
    }

    pub fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        files: &HashMap<String, PathBuf>,
        conditions: I,
        data_types: &HashMap<String, sqlparser::ast::DataType>,
    ) -> Result<Self, anyhow::Error> {
        let mut checks = CheckCollection::determine_checks(conditions, data_types)?;
        let tracked_columns = CheckCollection::determine_tracked_columns(&checks);
        let mut referenced_columns = CheckCollection::determine_referenced_columns(&checks);

        for (table, _) in tracked_columns.iter() {
            if !referenced_columns.contains_key(table) {
                referenced_columns.insert(table.to_owned(), Vec::new());
            }
            if !checks.contains_key(table) {
                checks.insert(table.to_owned(), Vec::new());
            }
        }

        Ok(CheckCollection {
            files: files.clone(),
            checks,
            tracked_columns,
            referenced_columns,
        })
    }

    fn determine_row_checks(&self) -> Result<HashMap<String, RowType<'_>>, anyhow::Error> {
        let mut res: HashMap<String, RowType<'_>> = HashMap::new();
        for (table, tracked_columns) in self.tracked_columns.iter() {
            let checks = &self.checks[table];
            let file = &self.files[table];
            let referenced_columns = &self.referenced_columns[table];
            res.insert(table.to_owned(), RowType::from(RowCheck::from_config(table, file, tracked_columns, checks, referenced_columns)?));
        }
        Ok(res)
    }
}

pub fn from_config<'a>(
    collection: &'a CheckCollection,
) -> Result<HashMap<String, RowType<'a>>, anyhow::Error> {
    collection.determine_row_checks()
}

use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::rc::{Rc, Weak};

use crate::column::ColumnMeta;
use crate::checks::{PlainCheckType, determine_target_tables, new_plain_test, parse_test_definition};
use crate::scanner::process_table_inserts;

pub trait Dependency {
    fn set_fulfilled_at_depth(&mut self, depth: &usize);
    fn has_been_fulfilled(&self) -> bool;

    fn get_dependencies(&self) -> &[Weak<RefCell<dyn Dependency>>];

    fn has_fulfilled_dependencies(&self) -> bool {
        self.get_dependencies().iter().all(|d| {
            d.upgrade().unwrap().borrow().has_been_fulfilled()
        })
    }

    fn fulfill_dependency(&mut self, depth: &usize) {
        if !self.has_been_fulfilled() {
            self.set_fulfilled_at_depth(depth);
        }
        assert!(self.has_been_fulfilled());
    }
}

fn process_inserts<'a, C: Iterator<Item=&'a PlainCheckType>>(
    working_file_path: &Path,
    checks: C,
    tracked_columns: &[&str],
    lookup_table: &HashMap<String, HashSet<String>>,
) -> Result<HashMap<String, HashSet<String>>, anyhow::Error> {
    let checks: Vec<&PlainCheckType> = checks.collect();
    if checks.is_empty() {
        return Err(anyhow::anyhow!("no checks"));
    }
    let mut tables: Vec<&str> = checks.iter().map(|c| c.get_table_name()).collect();
    tables.dedup();
    if tables.len() != 1 {
        return Err(anyhow::anyhow!("checks for multiple tables"));
    }
    let table = tables[0];

    let captured = process_table_inserts(working_file_path, table, tracked_columns, |statement| {
        let value_per_field = statement.get_values()?;
        if checks.iter().all(|t| {
            let col_name = t.get_column_name();
            t.test(col_name, &value_per_field[col_name], lookup_table)
        }) {
            return Ok(Some(()));
        }

        Ok(None)
    })?;
    Ok(captured)
}

type ColumnType = ColumnMeta;
pub type TrackedColumnType = ColumnMeta;
type DependencyType = Weak<RefCell<dyn Dependency>>;

impl From<&ColumnMeta> for ColumnType {
    fn from(c: &ColumnMeta) -> Self {
        c.to_owned()
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    // pub columns: HashMap<String, ColumnMeta>,
    foreign_tables: Vec<String>,
    references: Vec<String>,
    checks: Vec<PlainCheckType>,
    dependencies: Vec<DependencyType>,
    tested_at_pass: Option<usize>,
}

impl Dependency for TableMeta {
    fn get_dependencies(&self) -> &[Weak<RefCell<dyn Dependency>>] {
        &self.dependencies
    }

    fn set_fulfilled_at_depth(&mut self, depth: &usize) {
        self.tested_at_pass = Some(depth.to_owned());
    }

    fn has_been_fulfilled(&self) -> bool {
        self.tested_at_pass.is_some()
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
    fn new(table: &str, check_definitions: &[String], references: &[String]) -> Result<Rc<RefCell<Self>>, anyhow::Error> {
        let mut checks = Vec::new();
        let mut foreign_tables = Vec::new();

        for check in check_definitions {
            checks.push(new_plain_test(table, check)?);
            for t in determine_target_tables(check)? {
                foreign_tables.push(t.to_owned());
                foreign_tables.dedup();
            }
        }
        Ok(Rc::new(RefCell::new(TableMeta {
            table: table.to_owned(),
            foreign_tables,
            references: Vec::from(references),
            checks,
            dependencies: Vec::new(),
            tested_at_pass: None,
        })))
    }

    fn set_checks_and_references(&mut self, checks: &[String], references: &[String]) -> Result<(), anyhow::Error> {
        self.references = Vec::from(references);
        for check in checks {
            self.checks.push(new_plain_test(&self.table, check)?);
            for t in determine_target_tables(check)? {
                self.foreign_tables.push(t.to_owned());
                self.foreign_tables.dedup();
            }
        }
        Ok(())
    }

    fn add_column_meta(&mut self, elem: ColumnMeta) {
        if self.table.is_empty() {
            self.table = elem.get_table_name().to_owned();
        } else if self.table != elem.get_table_name() {
            panic!("mismatched table names");
        }
    }

    pub fn get_foreign_tables(&self) -> Vec<String> {
        self.foreign_tables.clone()
    }

    fn add_dependency(&mut self, target: &Rc<RefCell<TableMeta>>) {
        let weak = Rc::downgrade(target);
        self.dependencies.push(weak);
    }

    fn get_checks(&self) -> impl Iterator<Item=&PlainCheckType> {
        self.checks.iter()
    }

    fn get_tracked_columns(&self) -> Vec<&str> {
        self.references.iter().map(|x| x.as_str()).collect()
    }


    pub fn process_data_file(
        &mut self,
        current_pass: &usize,
        lookup_table: &HashMap<String, HashSet<String>>,
        working_file_path: &Path,
    ) -> Result<Option<HashMap<String, HashSet<String>>>, anyhow::Error> {
        if !self.has_fulfilled_dependencies() {
            println!("Skipping table {} since it still has dependencies", &self.table);
            return Ok(None);
        }

        let captured = process_inserts(working_file_path, self.get_checks(), &self.get_tracked_columns(), lookup_table)?;

        self.fulfill_dependency(current_pass);

        Ok(Some(captured))
    }
}

fn determine_checks_per_table(definitions: &[(String, String)]) -> Result<HashMap<String, Vec<String>>, anyhow::Error> {
    let mut checks: HashMap<String, Vec<String>> = HashMap::new();
    for (table, definition) in definitions.iter() {
        if !checks.contains_key(table) {
            checks.insert(table.to_owned(), Vec::new());
        }

        let Some(t_checks) = checks.get_mut(table) else {
            return Err(anyhow::anyhow!("cannot get references of table"));
        };
        t_checks.push(definition.to_owned());
        t_checks.dedup();
    }
    Ok(checks)
}

fn determine_references_per_table(definitions: &[(String, String)]) -> Result<HashMap<String, Vec<String>>, anyhow::Error> {
    let mut references: HashMap<String, Vec<String>> = HashMap::new();
    for (table, definition) in definitions.iter() {
        let (_, deps) = parse_test_definition(definition)?;
        if !references.contains_key(table) {
            references.insert(table.to_owned(), Vec::new());
        }

        for key in deps.iter() {
            let mut split = key.split('.');
            dbg!(&key);
            let (Some(target_table), Some(_), None) = (split.next(), split.next(), split.next()) else {
                return Err(anyhow::anyhow!("malformed key {}", key));
            };
            if !references.contains_key(target_table) {
                references.insert(target_table.to_owned(), Vec::new());
            }

            let Some(refs) = references.get_mut(target_table) else {
                return Err(anyhow::anyhow!("cannot get references of table"));
            };
            refs.push(key.to_owned());
            refs.dedup();
        }
    }
    Ok(references)
}

#[derive(Debug)]
pub struct CheckCollection {
    table_meta: HashMap<String, Rc<RefCell<TableMeta>>>,
}

impl CheckCollection {
    pub fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
    ) -> Result<Self, anyhow::Error> {
        let definitions: Vec<(String, String)> = conditions.flat_map(|(table, conds)| {
            conds.iter().map(|c| (table.to_owned(), c.to_owned()))
        }).collect();

        let checks = determine_checks_per_table(&definitions)?;
        let references = determine_references_per_table(&definitions)?;

        let mut all_tables: HashSet<String> = HashSet::new();
        for (table, definition) in definitions.iter() {
            all_tables.insert(table.to_owned());
            let target_tables = determine_target_tables(definition)?;
            for target_table in target_tables {
                all_tables.insert(target_table.to_owned());
            }
        }

        let mut tracked_cols: Vec<TrackedColumnType> = Vec::new();

        for (table, definition) in definitions.iter() {
            let (column_name, deps) = parse_test_definition(definition)?;
            let mut column_meta = ColumnMeta::new(table, &column_name, &deps)?;
            column_meta.add_check(definition);
            tracked_cols.push(column_meta);

            // track target columns
            for key in deps.iter() {
                tracked_cols.push(ColumnMeta::from_foreign_key(key)?);
            }
        }

        let grouped: HashMap<String, Rc<RefCell<TableMeta>>> = tracked_cols
            .into_iter()
            .into_grouping_map_by(|t| t.get_table_name().to_owned())
            .collect();


        // set checks
        for table_meta in grouped.values() {
            let table = table_meta.borrow().table.to_owned();
            table_meta.borrow_mut().set_checks_and_references(&checks[&table], &references[&table])?;
        }

        // set dependencies
        for table_meta in grouped.values() {
            let foreign_tables = table_meta.borrow().get_foreign_tables();
            for target_table in foreign_tables.iter() {
                let target_table_meta = &grouped[target_table];
                table_meta.borrow_mut().add_dependency(target_table_meta);
            }
        }

        dbg!(&grouped);
        dbg!(&all_tables);
        Ok(CheckCollection {
            table_meta: grouped,
        })
    }

    fn get_pending_tables(&self) -> Vec<String>{
        self.table_meta.values().filter(|v| !v.borrow().has_been_fulfilled()).map(|v| v.borrow().table.to_owned()).collect()
    }

    pub fn process(
        &mut self,
        working_file_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let mut current_pass = 1;
        let mut lookup_table = HashMap::new();
        while !self.get_pending_tables().is_empty() {
            let pending = self.get_pending_tables();
            println!("Running pass {current_pass}");
            dbg!(&pending);
            dbg!(&lookup_table);
            for table_meta in self.table_meta.values_mut().filter(|t| pending.iter().any(|p| p == &t.borrow().table)) {
                let captured_option = table_meta.borrow_mut().process_data_file(
                    &current_pass,
                    &lookup_table,
                    working_file_path,
                )?;
                if let Some(captured) = captured_option {
                    lookup_table.extend(captured);
                }
            }
            current_pass += 1;
        }
        dbg!(&lookup_table);
        Ok(())
    }
}

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::rc::{Rc, Weak};

use crate::checks::{PlainCheckType, TableChecks, determine_all_checked_tables, determine_checks_per_table, determine_references_per_table};
use crate::scanner::process_table_inserts;

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

type TableMetaCell = Rc<RefCell<TableMeta>>;
type WeakDependencyType = Weak<RefCell<TableMeta>>;

#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    foreign_tables: Vec<String>,
    references: Vec<String>,
    checks: Vec<PlainCheckType>,
    dependencies: Vec<WeakDependencyType>,
    tested_at_pass: Option<usize>,
}

impl TryFrom<TableChecks> for TableMetaCell {
    type Error = anyhow::Error;
    fn try_from(table_checks: TableChecks) -> Result<Self, Self::Error> {
        let checks = table_checks.get_checks()?;
        Ok(Rc::new(RefCell::new(TableMeta {
            table: table_checks.table,
            foreign_tables: table_checks.foreign_tables,
            references: table_checks.references,
            checks,
            dependencies: Vec::new(),
            tested_at_pass: None,
        })))
    }
}

impl TableMeta {
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

    fn has_been_fulfilled(&self) -> bool {
        self.tested_at_pass.is_some()
    }

    fn has_fulfilled_dependencies(&self) -> bool {
        self.dependencies.iter().all(|d| {
            d.upgrade().unwrap().borrow().has_been_fulfilled()
        })
    }

    fn fulfill_dependency(&mut self, depth: &usize) {
        if !self.has_been_fulfilled() {
            self.tested_at_pass = Some(depth.to_owned());
        }
        assert!(self.has_been_fulfilled());
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
        let all_tables = determine_all_checked_tables(&definitions)?;

        let mut grouped: HashMap<String, Rc<RefCell<TableMeta>>> = HashMap::new();
        for table in all_tables.iter() {
            let table_checks = TableChecks::new(table, &checks[table], &references[table])?;
            grouped.insert(table.to_owned(), TableMetaCell::try_from(table_checks)?);
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

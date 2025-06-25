use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::rc::{Rc, Weak};


use crate::checks::{determine_target_tables, get_checks_per_table, test_checks, PlainCheckType, TableChecks};
use crate::scanner::process_table_inserts;
use crate::dependencies::DependencyNode;

fn process_inserts(
    working_file_path: &Path,
    table: &str,
    checks: &[PlainCheckType],
    tracked_columns: &[&str],
    lookup_table: &HashMap<String, HashSet<String>>,
) -> Result<HashMap<String, HashSet<String>>, anyhow::Error> {
    let captured = process_table_inserts(working_file_path, table, tracked_columns, |statement| {
        let value_per_field = statement.get_values()?;

        match test_checks(checks, value_per_field, lookup_table)? {
            false => Ok(None),
            true => Ok(Some(()))
        }
    })?;
    Ok(captured)
}

type TableMetaCell = Rc<RefCell<TableMeta>>;
type DependencyCell = Rc<RefCell<Dependency>>;
type WeakDependencyRef = Weak<RefCell<Dependency>>;

#[derive(Debug)]
#[derive(Default)]
struct Dependency {
    dependencies: Vec<WeakDependencyRef>,
    tested_at_pass: Option<usize>,
}

impl Dependency {
    fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Dependency { dependencies: Vec::new(), tested_at_pass: None }))
    }

    fn add_dependency(&mut self, target: &Rc<RefCell<Dependency>>) {
        let weak = Rc::downgrade(target);
        self.dependencies.push(weak);
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
}


#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    foreign_tables: Vec<String>,
    references: Vec<String>,
    checks: Vec<PlainCheckType>,
    dependency: DependencyCell,
}

impl TryFrom<&TableChecks> for TableMetaCell {
    type Error = anyhow::Error;
    fn try_from(table_checks: &TableChecks) -> Result<Self, Self::Error> {
        let checks = table_checks.get_checks()?;
        Ok(Rc::new(RefCell::new(TableMeta {
            table: table_checks.table.clone(),
            foreign_tables: table_checks.foreign_tables.clone(),
            references: table_checks.references.clone(),
            checks,
            dependency: Dependency::new(),
        })))
    }
}

impl TableMeta {
    pub fn get_foreign_tables(&self) -> Vec<String> {
        self.foreign_tables.clone()
    }

    fn get_tracked_columns(&self) -> Vec<&str> {
        self.references.iter().map(|x| x.as_str()).collect()
    }

    fn add_dependency(&mut self, target: &Rc<RefCell<TableMeta>>) {
        self.dependency.borrow_mut().add_dependency(&target.borrow().dependency);
    }

    pub fn process_data_file(
        &mut self,
        current_pass: &usize,
        lookup_table: &HashMap<String, HashSet<String>>,
        working_file_path: &Path,
    ) -> Result<Option<HashMap<String, HashSet<String>>>, anyhow::Error> {
        if !self.dependency.borrow().has_fulfilled_dependencies() {
            println!("Skipping table {} since it still has dependencies", &self.table);
            return Ok(None);
        }

        let captured = process_inserts(working_file_path, &self.table, &self.checks, &self.get_tracked_columns(), lookup_table)?;

        self.dependency.borrow_mut().fulfill_dependency(current_pass);

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

        let mut root = DependencyNode::new();
        for (table, definition) in definitions.iter() {
            root.add_child(table);
            let target_tables = determine_target_tables(definition)?;
            for target_table in target_tables {
                root.move_under(&target_table, table)?;
            }
        }

        dbg!(&root);
        dbg!(&root.group_by_depth());
        panic!("stop");


        let checks_per_table = get_checks_per_table(&definitions)?;

        let mut grouped: HashMap<String, Rc<RefCell<TableMeta>>> = HashMap::new();
        for (table, checks) in checks_per_table.iter() {
            grouped.insert(table.to_owned(), TableMetaCell::try_from(checks)?);
        }

        // set dependencies
        for table_meta in grouped.values() {
            let foreign_tables = table_meta.borrow().get_foreign_tables();
            for target_table in foreign_tables.iter() {
                let target_table_meta = &grouped[target_table];
                table_meta.borrow_mut().add_dependency(target_table_meta);
            }
        }

        Ok(CheckCollection {
            table_meta: grouped,
        })
    }

    fn get_pending_tables(&self) -> Vec<String>{
        self.table_meta.values().filter(|v| !v.borrow().dependency.borrow().has_been_fulfilled()).map(|v| v.borrow().table.to_owned()).collect()
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

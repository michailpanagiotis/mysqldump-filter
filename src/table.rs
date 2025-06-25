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


#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    foreign_tables: Vec<String>,
    references: Vec<String>,
    checks: Vec<PlainCheckType>,
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

    fn process_data_file(
        &mut self,
        lookup_table: &HashMap<String, HashSet<String>>,
        working_file_path: &Path,
    ) -> Result<Option<HashMap<String, HashSet<String>>>, anyhow::Error> {
        let captured = process_inserts(working_file_path, &self.table, &self.checks, &self.get_tracked_columns(), lookup_table)?;
        Ok(Some(captured))
    }
}

#[derive(Debug)]
pub struct CheckCollection {
    table_meta: HashMap<String, Rc<RefCell<TableMeta>>>,
    grouped_tables: Vec<HashSet<String>>,
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

        let checks_per_table = get_checks_per_table(&definitions)?;

        let mut grouped: HashMap<String, Rc<RefCell<TableMeta>>> = HashMap::new();
        for (table, checks) in checks_per_table.iter() {
            grouped.insert(table.to_owned(), TableMetaCell::try_from(checks)?);
        }

        Ok(CheckCollection {
            table_meta: grouped,
            grouped_tables: root.group_by_depth(),
        })
    }

    pub fn process(
        &mut self,
        working_file_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let mut current_pass = 1;
        let mut lookup_table = HashMap::new();
        for pending in self.grouped_tables.iter() {
            println!("Running pass {current_pass}");
            dbg!(&pending);
            dbg!(&lookup_table);
            for table_meta in self.table_meta.values_mut().filter(|t| pending.iter().any(|p| p == &t.borrow().table)) {
                let captured_option = table_meta.borrow_mut().process_data_file(
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

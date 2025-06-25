use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;


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


#[derive(Debug)]
#[derive(Default)]
pub struct TableMeta {
    pub table: String,
    references: Vec<String>,
    checks: Vec<PlainCheckType>,
}

impl TryFrom<&TableChecks> for TableMeta {
    type Error = anyhow::Error;
    fn try_from(table_checks: &TableChecks) -> Result<Self, Self::Error> {
        let checks = table_checks.get_checks()?;
        Ok(TableMeta {
            table: table_checks.table.clone(),
            references: table_checks.references.clone(),
            checks,
        })
    }
}

impl TableMeta {
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
    table_meta: HashMap<String, TableMeta>,
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

        let mut grouped: HashMap<String, TableMeta> = HashMap::new();
        for (table, checks) in checks_per_table.iter() {
            grouped.insert(table.to_owned(), TableMeta::try_from(checks)?);
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
            for table_meta in self.table_meta.values_mut().filter(|t| pending.iter().any(|p| p == &t.table)) {
                let captured_option = table_meta.process_data_file(
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

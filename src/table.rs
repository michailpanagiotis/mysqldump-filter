use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;


use crate::checks::{get_checks_per_table, test_checks, TableChecks};
use crate::scanner::process_table_inserts;
use crate::dependencies::get_dependency_order;


fn process_data_file(
    table: &str,
    table_checks: &TableChecks,
    lookup_table: &HashMap<String, HashSet<String>>,
    working_file_path: &Path,
) -> Result<Option<HashMap<String, HashSet<String>>>, anyhow::Error> {
    let checks = table_checks.get_checks()?;
    let tracked_columns: Vec<&str> = table_checks.references.iter().map(|x| x.as_str()).collect();
    let captured = process_table_inserts(working_file_path, table, &tracked_columns, |statement| {
        let value_per_field = statement.get_values()?;

        match test_checks(&checks, value_per_field, lookup_table)? {
            false => Ok(None),
            true => Ok(Some(()))
        }
    })?;
    Ok(Some(captured))
}

#[derive(Debug)]
pub struct CheckCollection {
    table_checks: HashMap<String, TableChecks>,
    dependency_order: Vec<HashSet<String>>,
}

impl CheckCollection {
    pub fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
    ) -> Result<Self, anyhow::Error> {
        let definitions: Vec<(String, String)> = conditions.flat_map(|(table, conds)| {
            conds.iter().map(|c| (table.to_owned(), c.to_owned()))
        }).collect();


        let checks_per_table = get_checks_per_table(&definitions)?;

        Ok(CheckCollection {
            dependency_order: get_dependency_order(&definitions)?,
            table_checks: checks_per_table,
        })
    }

    pub fn process(
        &mut self,
        working_file_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let mut current_pass = 1;
        let mut lookup_table = HashMap::new();

        let mut passes = Vec::new();
        for tables in self.dependency_order.iter() {
            let mut checks: HashMap<String, &TableChecks> = HashMap::new();
            for table in tables {
                let table_checks = self.table_checks.get(table).ok_or(anyhow::anyhow!("cannot find checks"))?;
                checks.insert(table.to_owned(), table_checks);
            }
            passes.push(checks);
        }

        dbg!(&passes);

        for pending in &passes {
            println!("Running pass {current_pass}");
            dbg!(&pending);
            dbg!(&lookup_table);
            for (table, table_checks) in pending.iter() {
                let captured_option = process_data_file(
                    &table,
                    &table_checks,
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

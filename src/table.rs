use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;

use crate::checks::{get_passes, test_checks, PlainCheckType};
use crate::scanner::process_table_inserts;

fn process_data_file(
    checks: &[PlainCheckType],
    lookup_table: &HashMap<String, HashSet<String>>,
    working_file_path: &Path,
) -> Result<Option<HashMap<String, HashSet<String>>>, anyhow::Error> {
    let tracked_columns: Vec<String> = checks.iter().flat_map(|c| c.get_tracked_columns()).collect();
    let tables: HashSet<&str> = checks.iter().map(|c| c.get_table_name()).collect();
    if tables.len() != 1 {
        Err(anyhow::anyhow!("cannot perform checks on multiple tables at once"))?;
    }
    let Some(table) = tables.iter().next() else { Err(anyhow::anyhow!("cannot find table"))? };
    let captured = process_table_inserts(working_file_path, table, &tracked_columns, |statement| {
        let value_per_field = statement.get_values()?;

        match test_checks(checks, value_per_field, lookup_table)? {
            false => Ok(None),
            true => Ok(Some(()))
        }
    })?;
    Ok(Some(captured))
}

#[derive(Debug)]
pub struct CheckCollection {
    definitions: Vec<(String, String)>,
}

impl CheckCollection {
    pub fn new<'a, I: Iterator<Item=(&'a String, &'a Vec<String>)>>(
        conditions: I,
    ) -> Result<Self, anyhow::Error> {
        let definitions: Vec<(String, String)> = conditions.flat_map(|(table, conds)| {
            conds.iter().map(|c| (table.to_owned(), c.to_owned()))
        }).collect();

        Ok(CheckCollection {
            definitions: definitions.clone(),
        })
    }

    pub fn process(
        &mut self,
        working_file_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let mut current_pass = 1;
        let mut lookup_table = HashMap::new();

        let passes = get_passes(&self.definitions)?;

        dbg!(&passes);

        for pending in &passes {
            println!("Running pass {current_pass}");
            dbg!(&pending);
            dbg!(&lookup_table);
            for table_checks in pending.iter() {
                if let Some(captured) = process_data_file(
                    table_checks,
                    &lookup_table,
                    working_file_path,
                )? {
                    lookup_table.extend(captured);
                }
            }
            current_pass += 1;
        }
        dbg!(&lookup_table);
        Ok(())
    }
}

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::PlainCheckType;
use crate::scanner::process_table_inserts;


pub fn process_checks(passes: &[Vec<Vec<PlainCheckType>>], working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut current_pass = 1;
    let mut lookup_table = HashMap::new();

    for pending in passes {
        println!("Running pass {current_pass}");
        dbg!(&pending);
        dbg!(&lookup_table);
        for checks in pending.iter() {
            let tracked_columns: Vec<String> = checks.iter().flat_map(|c| c.get_tracked_columns()).collect();
            let tables: HashSet<&str> = checks.iter().map(|c| c.get_table_name()).collect();
            if tables.len() != 1 {
                Err(anyhow::anyhow!("cannot perform checks on multiple tables at once"))?;
            }
            let Some(table) = tables.iter().next() else { Err(anyhow::anyhow!("cannot find table"))? };
            let captured = process_table_inserts(working_file_path, table, &tracked_columns, |statement| {
                let value_per_field = statement.get_values()?;

                for check in checks.iter() {
                    if !check.test(value_per_field, &lookup_table)? {
                        return Ok(None);
                    }
                }

                Ok(Some(()))
            })?;

            lookup_table.extend(captured);
        }
        current_pass += 1;
    }
    dbg!(&lookup_table);
    Ok(())
}

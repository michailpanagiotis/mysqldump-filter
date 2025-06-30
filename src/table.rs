use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::PassChecks;
use crate::scanner::process_table_inserts;


pub fn process_checks(passes: &[PassChecks], working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();

    dbg!(&passes);

    for pending in passes {
        dbg!(&pending);
        dbg!(&lookup_table);
        for checks in pending.0.iter() {
            let tracked_columns: Vec<String> = checks.0.iter().flat_map(|c| c.get_tracked_columns()).collect();
            let table = checks.get_table()?;
            let captured = process_table_inserts(working_file_path, table, &tracked_columns, |statement| {
                let value_per_field = statement.get_values()?;

                for check in checks.0.iter() {
                    if !check.test(value_per_field, &lookup_table)? {
                        return Ok(None);
                    }
                }

                Ok(Some(()))
            })?;

            lookup_table.extend(captured);
        }
    }
    Ok(())
}

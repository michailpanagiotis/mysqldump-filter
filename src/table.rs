use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::{DBChecks, TableChecks};
use crate::scanner::{TransformFn, process_table_inserts};

fn get_table_transform_fn(table_checks: &TableChecks, lookup_table: &HashMap<String, HashSet<String>>) -> impl TransformFn {
    |statement| {
        let Ok(value_per_field) = statement.try_into() else { Err(anyhow::anyhow!("cannot parse values"))? };
        for check in table_checks.0.iter() {
            if !check.test(value_per_field, lookup_table)? {
                return Ok(None);
            }
        }
        Ok(Some(HashMap::new()))
    }
}

pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending_tables in passes {
        dbg!(&lookup_table);
        for table_checks in pending_tables {
            let transform_fn = get_table_transform_fn(&table_checks, &lookup_table);
            let tracked_columns = table_checks.get_tracked_columns();
            let table = table_checks.get_table()?;
            let captured = process_table_inserts(
                working_file_path,
                table,
                &tracked_columns,
                transform_fn,
            )?;

            lookup_table.extend(captured);
        }
    }
    Ok(())
}

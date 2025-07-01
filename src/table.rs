use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::{DBChecks, TableChecks};
use crate::scanner::{TransformFn, process_table_inserts};

fn get_transform<'a>(
    table_checks: &'a TableChecks,
    lookup_table: &'a HashMap<String, HashSet<String>>,
) -> Box<dyn TransformFn + 'a>{
    Box::new(|statement| {
        let Ok(value_per_field) = statement.try_into() else { Err(anyhow::anyhow!("cannot parse values"))? };
        for check in table_checks.0.iter() {
            if !check.test(value_per_field, lookup_table)? {
                return Ok(None);
            }
        }
        Ok(Some(HashMap::new()))
    })
}

pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending_tables in passes {
        dbg!(&lookup_table);
        for table_checks in pending_tables {
            let tracked_columns = table_checks.get_tracked_columns();
            let table = table_checks.get_table()?;
            let captured = process_table_inserts(
                table,
                &tracked_columns,
                working_file_path,
                get_transform(&table_checks, &lookup_table),
            )?;

            lookup_table.extend(captured);
        }
    }
    Ok(())
}

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::{DBChecks, TableChecks};
use crate::scanner::{TransformFn, process_table_inserts};

pub fn process_inserts<F>(
    checks: &TableChecks,
    working_file_path: &Path,
    transform: F,
) -> Result<HashMap<String, HashSet<String>>, anyhow::Error>
  where F: TransformFn
{
    let tracked_columns = checks.get_tracked_columns();
    let table = checks.get_table()?;
    let captured = process_table_inserts(table, &tracked_columns, working_file_path, transform)?;
    Ok(captured)
}

pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending in passes.0 {
        dbg!(&lookup_table);
        for checks in pending {
            let captured = process_inserts(&checks, working_file_path, |statement| {
                let value_per_field = statement.get_values()?;
                checks.test(value_per_field, &lookup_table)
            })?;

            lookup_table.extend(captured);
        }
    }
    Ok(())
}

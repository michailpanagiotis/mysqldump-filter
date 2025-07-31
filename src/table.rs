use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::{DBChecks, TableChecks};
use crate::scanner::{TransformFn, process_table_inserts};

fn get_table_transform_fn<'a>(table_checks: &'a TableChecks, lookup_table: &'a mut HashMap<String, HashSet<String>>) -> (Vec<&'a str>, impl TransformFn) {
    (table_checks.get_tracked_columns(), |statement| {
        table_checks.transform_statement(lookup_table, statement)
    })
}

pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending_tables in passes {
        dbg!(&lookup_table);
        for (table, table_checks) in pending_tables {
            let (tracked_columns, transform_fn) = get_table_transform_fn(&table_checks, &mut lookup_table);
            process_table_inserts(
                working_file_path,
                &table,
                &table_checks.get_tracked_columns(),
                transform_fn,
            )?;
        }
    }
    Ok(())
}

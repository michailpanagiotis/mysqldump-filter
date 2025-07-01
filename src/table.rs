use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::{DBChecks, TableChecks};
use crate::scanner::{TransformFn, process_table_inserts};

pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending_tables in passes {
        dbg!(&lookup_table);
        for table_checks in pending_tables {
            let tracked_columns = table_checks.get_tracked_columns();
            let table = table_checks.get_table()?;
            let transform: Box<dyn TransformFn> = Box::new(|statement| {
                table_checks.test(statement, &lookup_table)
            });
            let captured = process_table_inserts(table, &tracked_columns, working_file_path, transform)?;

            lookup_table.extend(captured);
        }
    }
    Ok(())
}

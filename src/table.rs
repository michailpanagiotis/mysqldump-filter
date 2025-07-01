use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::{DBChecks, TableChecks};
use crate::scanner::{TransformFn, process_table_inserts};

pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending in passes {
        dbg!(&lookup_table);
        for checks in pending {
            let tracked_columns = checks.get_tracked_columns();
            let table = checks.get_table()?;
            let transform: Box<dyn TransformFn> = Box::new(|statement| {
                checks.test(statement, &lookup_table)
            });
            let captured = process_table_inserts(table, &tracked_columns, working_file_path, transform)?;

            lookup_table.extend(captured);
        }
    }
    Ok(())
}

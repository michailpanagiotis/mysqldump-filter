use std::collections::HashMap;
use std::path::Path;

use crate::checks::DBChecks;
use crate::scanner::process_table_inserts;


pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending_tables in passes {
        dbg!(&lookup_table);
        for (table, table_checks) in pending_tables {
            process_table_inserts(
                working_file_path,
                &table,
                |statement| {
                    table_checks.apply(statement, &mut lookup_table)
                },
            )?;
        }
    }
    Ok(())
}

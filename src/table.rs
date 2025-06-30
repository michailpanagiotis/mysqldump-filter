use std::collections::HashMap;
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
            let tracked_columns = checks.get_tracked_columns();
            let table = checks.get_table()?;
            let captured = process_table_inserts(working_file_path, table, &tracked_columns, |statement| {
                let value_per_field = statement.get_values()?;

                if !checks.test(value_per_field, &lookup_table)? {
                    return Ok(None);
                }

                Ok(Some(()))
            })?;

            lookup_table.extend(captured);
        }
    }
    Ok(())
}

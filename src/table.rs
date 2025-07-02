use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::checks::{DBChecks, TableChecks};
use crate::scanner::{process_table_inserts, TransformArguments, TransformFn};

fn get_table_transform_fn<'a>(table_checks: &'a TableChecks, lookup_table: &'a HashMap<String, HashSet<String>>) -> (&'a str, Vec<&'a str>, impl TransformFn) {
    (table_checks.get_table(), table_checks.get_tracked_columns(), table_checks.get_update_fn::<TransformArguments>(lookup_table))
}

// fn get_table_transform_fn(table_checks: &TableChecks, lookup_table: &HashMap<String, HashSet<String>>) -> impl TransformFn {
//     |statement| {
//         let Ok(value_per_field) = statement.try_into() else { Err(anyhow::anyhow!("cannot parse values"))? };
//         for check in table_checks.0.iter() {
//             if !check.test(value_per_field, lookup_table)? {
//                 return Ok(None);
//             }
//         }
//         Ok(Some(HashMap::new()))
//     }
// }

pub fn process_checks(passes: DBChecks, working_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut lookup_table = HashMap::new();
    for pending_tables in passes {
        dbg!(&lookup_table);
        // let transforms: HashMap<String, Box<dyn TransformFn>> = pending_tables.0.iter().map(|cks| (cks.get_table().to_owned(), get_table_transform_fn(&cks, &lookup_table))).collect();
        for table_checks in pending_tables {
            let (table, tracked_columns, transform_fn) = get_table_transform_fn(&table_checks, &lookup_table);
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

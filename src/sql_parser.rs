use std::path::{Path, PathBuf};

use itertools::Itertools;

use crate::sql_statement::Statement;
use crate::io_utils::SQLWriter;
use crate::trackers::ReferenceTracker;
use crate::config::Config;

pub fn parse_input_file(config: &Config, input_file: &Path, output_file: &Path) {
    let mut filepaths: Vec<PathBuf> = Vec::new();
    let mut reference_trackers: Vec<ReferenceTracker> = Vec::new();
    for (table, statements) in Statement::from_file(input_file, &config.requested_tables).chunk_by(Statement::get_table).into_iter() {
        let iter = config.get_table_iterator(&table, statements);

        let working_dir_path = &config.working_dir_path.clone();
        let schema_file = &config.schema_file.clone();
        let table_config = config.get_table_config(&table);


        let mut writer = table_config.get_writer(working_dir_path, schema_file);
        let mut ref_tracker = table_config.get_reference_tracker();
        if let Some(table) = &table_config.table {
            println!("Parsing table {}", &table);
        }
        for statement in iter {
            if let Some(ref mut tracker) = ref_tracker {
                tracker.capture(&statement);
            }
            writer.write_statement(&statement).expect("Unable to write data");
        };
        writer.flush().expect("Cannot flush buffer");

        filepaths.push(writer.get_filepath());
        if let Some(tracker) = ref_tracker {
            reference_trackers.push(tracker);
        }
    }

    let references = ReferenceTracker::merge(reference_trackers.iter());

    dbg!(&references);

    SQLWriter::combine_files(filepaths.iter(), output_file);
}

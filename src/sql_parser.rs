use std::io::Write;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::sql_statement::{FieldPositions, Statement};
use crate::io_utils::{WriterType, LineWriter, combine_files, read_sql};
use crate::config::{Config, FilterMap, TableFilters};

#[derive(Debug)]
struct InsertTracker {
    direct_filters: TableFilters,
    reference_filters: TableFilters,
    references: HashMap<String, HashSet<String>>,
    field_positions: FieldPositions,
}

impl InsertTracker {
    fn new(
        filters: &TableFilters,
        references: &[String],
        field_positions: FieldPositions,
    ) -> InsertTracker {
        InsertTracker {
            direct_filters: filters.to_direct_filters(),
            reference_filters: filters.to_reference_filters(),
            references: HashMap::from_iter(references.iter().map(|r| (r.clone(), HashSet::new()))),
            field_positions,
        }
    }

    fn should_drop_statement(&self, statement: &Statement) -> bool {
        if !statement.is_insert(){ return false };

        let value_per_field = statement.get_values(
            self.direct_filters.get_filtered_fields(),
            &self.field_positions,
        );

        !self.direct_filters.test(value_per_field)
    }

    fn capture_references(&mut self, statement: &Statement) {
        if !statement.is_insert(){ return };

        for (field, set) in self.references.iter_mut() {
            let value = self.field_positions.get_value(statement, field);
            set.insert(value.clone());
        }
    }

    fn flush(&self) {
        dbg!(&self.references);
    }
}

#[derive(Debug)]
struct TableDataWriter {
    filepath: PathBuf,
    writer: WriterType,
    insert_tracker: Option<InsertTracker>,
    filters: TableFilters,
    references: Vec<String>,
}

impl TableDataWriter {
    fn new(
        table: &String,
        working_dir: &Path,
        filters_per_table: &FilterMap,
        references_per_table: &HashMap<String, Vec<String>>,
    ) -> TableDataWriter {
        let filepath = working_dir.join(table).with_extension("sql");
        println!("Reading table {} into {}", table, filepath.display());
        let writer = LineWriter::new(&filepath);

        TableDataWriter {
            filepath,
            writer,
            insert_tracker: None,
            filters: filters_per_table.get(table),
            references: match references_per_table.get(table) {
                Some(x) => x.clone(),
                None => Vec::new(),
            },
        }
    }

    fn on_new_statement(&mut self, statement: &Statement) {
        if statement.is_insert() {
            if self.insert_tracker.is_none() {
                let field_positions = statement.get_field_positions().expect("cannot find positions");

                self.insert_tracker = Some(InsertTracker::new(
                    &self.filters, &self.references, field_positions,
                ))
            }
            let Some(ref mut insert_tracker) = self.insert_tracker else { return };
            if !insert_tracker.should_drop_statement(statement) {
                insert_tracker.capture_references(statement);
                self.writer.write_line(statement.as_bytes()).expect("Unable to write data");
            }
        } else {
            self.writer.write_line(statement.as_bytes()).expect("Unable to write data");
        }
    }

    fn flush(&mut self) {
        self.writer.flush().expect("Cannot flush buffer");
        if let Some(ref x) = self.insert_tracker {
            x.flush();
        }
    }
}

#[derive(Debug)]
pub struct Parser<'a> {
    config: &'a Config,
    writer_per_table: HashMap<String, TableDataWriter>,
    schema_writer: WriterType,
}

impl Parser<'_> {
    pub fn new(config: &Config) -> Parser {
        Parser{
            config,
            writer_per_table: HashMap::new(),
            schema_writer: LineWriter::new(&config.schema_file),
        }
    }

    fn register_table(&mut self, table: &String) {
        self.writer_per_table.insert(table.to_string(), TableDataWriter::new(
            table,
            &self.config.working_dir_path,
            &self.config.filters_per_table,
            &self.config.references_per_table,
        ));
    }

    fn on_new_statement(&mut self, statement: &Statement) {
        match statement.get_table() {
            None => {
                self.schema_writer.write_line(statement.as_bytes()).expect("Unable to write data");
            },
            Some(table) => {
                if !self.writer_per_table.contains_key(table) {
                    self.register_table(table);
                }
                let info = self.writer_per_table.get_mut(table).expect("Cannot find table info");
                info.on_new_statement(statement);
            },
        };
    }

    fn on_input_end(&mut self) {
        self.schema_writer.flush().expect("Unable to flush schema file");
        for info in self.writer_per_table.values_mut() {
            info.flush();
        }
    }

    fn get_data_files(&mut self) -> Vec<&Path> {
        self.writer_per_table.values().map(|x| x.filepath.as_path()).collect::<Vec<&Path>>()
    }

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        for (table, line) in read_sql(input_file, &self.config.requested_tables) {
            let statement = Statement::new(&table, &line);
            self.on_new_statement(&statement);
        }
        self.on_input_end();
        combine_files(
            &self.config.schema_file,
            self.get_data_files().into_iter(),
            output_file,
        );
    }
}

use std::io::Write;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::sql_statement::{FieldPositions, Statement};
use crate::io_utils::{WriterType, LineWriter, combine_files, read_sql};
use crate::config::{Config, FilterMap, TableFilters};


#[derive(Debug)]
struct ReferenceTracker {
    references: HashMap<String, HashSet<String>>,
    is_complete: bool,
}

impl ReferenceTracker {
    fn new() -> Self {
        ReferenceTracker {
            references: HashMap::new(),
            is_complete: false,
        }
    }

    fn get_key(&mut self, table: &String, field: &str) -> String {
        table.to_owned() + "." + field
    }

    fn has_completed(&self) -> bool {
        self.is_complete
    }

    fn insert(&mut self, table: &String, field: &str, value: &String) {
        let key: String = self.get_key(table, field);
        match self.references.get_mut(&key) {
            Some(x) => {
                x.insert(value.to_string());
            },
            None => {
                self.references.insert(key, HashSet::from([value.to_string()]));
            }
        }
    }
}

#[derive(Debug)]
struct InsertTracker {
    table: String,
    direct_filters: TableFilters,
    reference_filters: TableFilters,
    references: HashMap<String, HashSet<String>>,
    field_positions: FieldPositions,
}

impl InsertTracker {
    fn new(
        table: &String,
        filters: &TableFilters,
        references: &[String],
        field_positions: FieldPositions,
    ) -> InsertTracker {
        InsertTracker {
            table: table.to_string(),
            direct_filters: filters.to_direct_filters(),
            reference_filters: filters.to_reference_filters(),
            references: HashMap::from_iter(references.iter().map(|r| (r.clone(), HashSet::new()))),
            field_positions,
        }
    }

    fn should_drop_statement(&self, statement: &Statement, reference_tracker: &ReferenceTracker) -> bool {
        let value_per_field = self.field_positions.get_values(
            statement,
            self.direct_filters.get_filtered_fields(),
        );

        if !self.direct_filters.test(&value_per_field) {
            return false;
        }

        if reference_tracker.has_completed() {
            return self.reference_filters.test(&value_per_field);
        }

        true
    }

    fn capture_references(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) {
        for (field, set) in self.references.iter_mut() {
            let value = self.field_positions.get_value(statement, field);
            set.insert(value.clone());
            reference_tracker.insert(&self.table, field, &value);
        }
    }
}

#[derive(Debug)]
struct TableDataWriter {
    table: String,
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
            table: table.clone(),
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

    fn on_new_statement(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) {
        if statement.is_insert() {
            if self.insert_tracker.is_none() {
                let field_positions = statement.get_field_positions().expect("cannot find positions");

                self.insert_tracker = Some(InsertTracker::new(
                    &self.table, &self.filters, &self.references, field_positions,
                ))
            }
            let Some(ref mut insert_tracker) = self.insert_tracker else { return };
            if !insert_tracker.should_drop_statement(statement, reference_tracker) {
                insert_tracker.capture_references(statement, reference_tracker);
                self.writer.write_line(statement.as_bytes()).expect("Unable to write data");
            }
        } else {
            self.writer.write_line(statement.as_bytes()).expect("Unable to write data");
        }
    }

    fn flush(&mut self) {
        self.writer.flush().expect("Cannot flush buffer");
    }
}

#[derive(Debug)]
pub struct Parser<'a> {
    config: &'a Config,
    reference_tracker: ReferenceTracker,
    writer_per_table: HashMap<String, TableDataWriter>,
    schema_writer: WriterType,
}

impl Parser<'_> {
    pub fn new(config: &Config) -> Parser {
        Parser{
            config,
            reference_tracker: ReferenceTracker::new(),
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
                info.on_new_statement(statement, &mut self.reference_tracker);
            },
        };
    }

    fn on_input_end(&mut self) {
        self.schema_writer.flush().expect("Unable to flush schema file");
        for info in self.writer_per_table.values_mut() {
            info.flush();
        }
        dbg!(&self.reference_tracker);
    }

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        for (table, line) in read_sql(input_file, &self.config.requested_tables) {
            let statement = Statement::new(&table, &line);
            self.on_new_statement(&statement);
        }
        self.on_input_end();

        combine_files(
            std::iter::once(self.config.schema_file.clone()).chain(self.writer_per_table.values().map(|x| x.filepath.clone())),
            output_file,
        );
    }
}

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

        TableDataWriter {
            table: table.clone(),
            filepath,
            insert_tracker: None,
            filters: filters_per_table.get(table),
            references: match references_per_table.get(table) {
                Some(x) => x.clone(),
                None => Vec::new(),
            },
        }
    }

    fn should_keep_statement(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) -> bool {
        if !statement.is_insert() {
            return true;
        }

        let insert_tracker = self.insert_tracker.get_or_insert({
            let field_positions = statement.get_field_positions().expect("cannot find positions");

            InsertTracker::new(
                &self.table, &self.filters, &self.references, field_positions,
            )
        });

        if insert_tracker.should_drop_statement(statement, reference_tracker) {
            return false;
        }

        insert_tracker.capture_references(statement, reference_tracker);
        true
    }
}

#[derive(Debug)]
pub struct Parser<'a> {
    config: &'a Config,
    reference_tracker: ReferenceTracker,
    data_writer_per_table: HashMap<String, TableDataWriter>,
    schema_writer: WriterType,
    writer_per_table: HashMap<String, WriterType>,
}

impl Parser<'_> {
    pub fn new(config: &Config) -> Parser {
        Parser{
            config,
            reference_tracker: ReferenceTracker::new(),
            data_writer_per_table: HashMap::new(),
            schema_writer: LineWriter::new(&config.schema_file),
            writer_per_table: HashMap::new(),
        }
    }

    fn register_table(&mut self, table: &String) {
        let filepath = self.config.working_dir_path.join(table).with_extension("sql");
        println!("Reading table {} into {}", table, filepath.display());
        self.writer_per_table.insert(table.clone(), LineWriter::new(&filepath));
        self.data_writer_per_table.insert(table.to_string(), TableDataWriter::new(
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
                if !self.data_writer_per_table.contains_key(table) {
                    self.register_table(table);
                }
                let info = self.data_writer_per_table.get_mut(table).expect("Cannot find table info");
                if info.should_keep_statement(statement, &mut self.reference_tracker) {
                    self.writer_per_table.get_mut(table).unwrap().write_line(statement.as_bytes()).expect("Unable to write data");
                }
            },
        };
    }

    fn on_input_end(&mut self) {
        self.schema_writer.flush().expect("Unable to flush schema file");
        for writer in self.writer_per_table.values_mut() {
            writer.flush().expect("Cannot flush buffer");
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
            std::iter::once(self.config.schema_file.clone()).chain(self.data_writer_per_table.values().map(|x| x.filepath.clone())),
            output_file,
        );
    }
}

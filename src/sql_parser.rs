use std::io::Write;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use itertools::Itertools;

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
    direct_filters: TableFilters,
    reference_filters: TableFilters,
    references: HashMap<String, HashSet<String>>,
    field_positions: FieldPositions,
}

impl InsertTracker {
    fn new(
        table: &String,
        filters_per_table: &FilterMap,
        references_per_table: &HashMap<String, Vec<String>>,
        statement: &Statement,
    ) -> Self {

        dbg!(statement);
        let field_positions = statement.get_field_positions().expect("cannot find positions");

        let filters = filters_per_table.get(table);
        let references = match references_per_table.get(table) {
            Some(x) => x.clone(),
            None => Vec::new(),
        };
        InsertTracker {
            direct_filters: filters.to_direct_filters(),
            reference_filters: filters.to_reference_filters(),
            references: HashMap::from_iter(references.iter().map(|r| (r.clone(), HashSet::new()))),
            field_positions,
        }
    }

    fn capture_references(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) {
        if let Some(ref table) = statement.table {
            for (field, set) in self.references.iter_mut() {
                let value = self.field_positions.get_value(statement, field);
                set.insert(value.clone());
                reference_tracker.insert(table, field, &value);
            }
        }
    }

    fn should_keep_statement(&mut self, statement: &Statement, reference_tracker: &mut ReferenceTracker) -> bool {
        if !statement.is_insert() {
            return true;
        }

        let value_per_field = self.field_positions.get_values(
            statement,
            self.direct_filters.get_filtered_fields(),
        );

        if !self.direct_filters.test(&value_per_field) {
            return false;
        }

        if reference_tracker.has_completed() && !self.reference_filters.test(&value_per_field) {
            return false;
        }

        self.capture_references(statement, reference_tracker);
        true
    }
}

#[derive(Debug)]
pub struct Parser<'a> {
    config: &'a Config,
    reference_tracker: ReferenceTracker,
    insert_tracker_per_table: HashMap<String, InsertTracker>,
    schema_writer: WriterType,
    writer_per_table: HashMap<String, WriterType>,
    filepaths: Vec<PathBuf>,
}

impl Parser<'_> {
    pub fn new(config: &Config) -> Parser {
        Parser{
            config,
            reference_tracker: ReferenceTracker::new(),
            insert_tracker_per_table: HashMap::new(),
            schema_writer: LineWriter::new(&config.schema_file),
            writer_per_table: HashMap::new(),
            filepaths: Vec::from([config.schema_file.clone()]),
        }
    }

    fn register_table(&mut self, table: &String, statement: &Statement) {
        let filepath = self.config.working_dir_path.join(table).with_extension("sql");
        self.filepaths.push(filepath.clone());
        println!("Reading table {} into {}", table, filepath.display());
        self.writer_per_table.insert(table.clone(), LineWriter::new(&filepath));
        self.insert_tracker_per_table.insert(table.to_string(), InsertTracker::new(
            table,
            &self.config.filters_per_table,
            &self.config.references_per_table,
            statement,
        ));
    }

    fn should_keep_statement(&mut self, statement: &Statement) -> bool {
        match statement.get_table() {
            None => {
                return true;
            },
            Some(table) => {
                if (!statement.is_insert()) {
                    return false;
                }
                if !self.insert_tracker_per_table.contains_key(table) {
                    self.register_table(table, &statement);
                }
                let info = self.insert_tracker_per_table.get_mut(table).expect("Cannot find table info");
                if info.should_keep_statement(&statement, &mut self.reference_tracker) {
                    return true;
                }
            },
        };
        return false;
    }

    fn on_new_statement(&mut self, statement: Statement) {
        match statement.get_table() {
            None => {
                self.schema_writer.write_line(statement.as_bytes()).expect("Unable to write data");
            },
            Some(table) => {
                if !self.insert_tracker_per_table.contains_key(table) {
                    self.register_table(table, &statement);
                }
                let info = self.insert_tracker_per_table.get_mut(table).expect("Cannot find table info");
                if info.should_keep_statement(&statement, &mut self.reference_tracker) {
                    self.writer_per_table.get_mut(table).unwrap().write_line(statement.as_bytes()).expect("Unable to write data");
                }
            },
        };
    }

    fn get_writer(&mut self, table: Option<String>) -> WriterType {
        let filepath = match (table) {
            Some(x) => self.config.working_dir_path.join(x).with_extension("sql"),
            None => self.config.schema_file.clone()
        };
        self.filepaths.push(filepath.clone());
        println!("Writing into {}", filepath.display());
        LineWriter::new(&filepath)
    }

    fn on_input_end(&mut self) {
        self.schema_writer.flush().expect("Unable to flush schema file");
        for writer in self.writer_per_table.values_mut() {
            writer.flush().expect("Cannot flush buffer");
        }
        dbg!(&self.reference_tracker);
    }

    pub fn parse_input_file(&mut self, input_file: &Path, output_file: &Path) {
        for (table, statements) in Statement::from_file(input_file).chunk_by(|statement| {
            statement.table.clone()
        }).into_iter() {
            let mut writer = self.get_writer(table);
            for statement in statements {
                if self.should_keep_statement(&statement) {
                    writer.write_line(statement.as_bytes()).expect("Unable to write data");
                }
            }
        }

        self.on_input_end();

        combine_files(
            self.filepaths.iter(),
            output_file,
        );
    }
}

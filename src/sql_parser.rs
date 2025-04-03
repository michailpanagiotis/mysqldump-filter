use std::io::Write;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::sql_statement::Statement;
use crate::io_utils::{WriterType, LineWriter, combine_files, read_sql};
use crate::config::{Config, FilterCondition};

#[derive(Debug)]
struct TableInfo {
    filepath: PathBuf,
    filters: Vec<FilterCondition>,
    references: HashMap<String, HashSet<String>>,
    insert_statement_sample: Option<Statement>,
    value_position_per_field: Option<HashMap<String, usize>>,
}

impl TableInfo {
    fn new(
        table: &String,
        working_dir: &Path,
        filters: Option<&Vec<FilterCondition>>,
        references: Option<&Vec<String>>,
    ) -> TableInfo {
        let filepath = working_dir.join(table).with_extension("sql");
        println!("Reading table {} into {}", table, filepath.display());

        TableInfo {
            filepath,
            filters: match filters { Some(f) => f.clone(), None => Vec::new() },
            references: match references { Some(r) => {
                HashMap::from_iter(r.iter().map(|r| (r.clone(), HashSet::new())))
            }, None => HashMap::new() },
            insert_statement_sample: None,
            value_position_per_field: None,
        }
    }

    fn get_writer(&self) -> WriterType {
        LineWriter::new(&self.filepath)
    }

    fn try_determine_field_positions(&mut self, statement: &Statement) {
        if !self.filters.is_empty() && self.value_position_per_field.is_some() {
            self.insert_statement_sample = Some(statement.clone());
            self.value_position_per_field = statement.get_field_positions();
        }
    }

    fn should_drop_statement(&self, statement: &Statement) -> bool {
        if !statement.is_insert(){ return false };

        let Some(ref value_position_per_field) = self.value_position_per_field else { return false };

        let values = statement.get_values();

        let failed_filters = self.filters.iter().filter(|f| {
            let position = value_position_per_field[&f.field];
            !f.test(&values[position])
        });

        failed_filters.count() > 0
    }

    fn capture_references(&mut self, statement: &Statement) {
        if !statement.is_insert(){ return };
        let Some(ref value_position_per_field) = self.value_position_per_field else { return };

        let values = statement.get_values();

        for (field, set) in self.references.iter_mut() {
            let position = value_position_per_field[field];
            let value = &values[position];
            set.insert(value.clone());
        }
    }
}

#[derive(Debug)]
struct TableDataWriter {
    writer: WriterType,
    table_info: TableInfo,
}

impl TableDataWriter {
    fn new(
        table: &String,
        working_dir: &Path,
        filters_per_table: &HashMap<String, Vec<FilterCondition>>,
        references_per_table: &HashMap<String, Vec<String>>,
    ) -> TableDataWriter {
        let table_info = TableInfo::new(
            table,
            working_dir,
            filters_per_table.get(table),
            references_per_table.get(table),
        );
        let writer = table_info.get_writer();
        TableDataWriter {
            writer,
            table_info,
        }
    }

    fn on_new_statement(&mut self, statement: &Statement) {
        if statement.is_insert() {
            self.table_info.try_determine_field_positions(statement);
        }
        if !self.table_info.should_drop_statement(statement) {
            self.table_info.capture_references(statement);
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
        self.writer_per_table.values().map(|x| x.table_info.filepath.as_path()).collect::<Vec<&Path>>()
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

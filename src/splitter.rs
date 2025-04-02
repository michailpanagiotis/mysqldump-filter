use lazy_static::lazy_static;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};

use crate::reader;
use crate::config::{Config, FilterCondition};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

fn get_writer(filename: &PathBuf) -> BufWriter<File> {
    File::create(filename).expect("Unable to create file");
    let file = OpenOptions::new()
        .append(true)
        .open(filename)
        .expect("Unable to open file");

    BufWriter::new(file)
}


#[derive(Debug)]
struct TableDataWriter {
    value_position_per_field: Option<HashMap<String, usize>>,
    filepath: PathBuf,
    writer: io::BufWriter<File>,
    filters: Option<Vec<FilterCondition>>,
}

impl TableDataWriter {
    fn new(table: &String, config: &Config) -> TableDataWriter {
        let path = config.output_dir.join(table).with_extension("sql");
        println!("Reading table {} into {}", table, path.display());
        TableDataWriter {
            value_position_per_field: None,
            filepath: path.clone(),
            writer: get_writer(&path),
            filters: config.filter_per_table.get(table).cloned(),
        }
    }

    fn on_new_statement(&mut self, reader::Statement { line, table: _, r#type: s_type }: &reader::Statement) {
        if self.filters.is_some() && self.value_position_per_field.is_none() && s_type == &reader::StatementType::Insert {
            self.value_position_per_field = Some(reader::parse_fields(line));
        }
        self.writer.write_all(line.as_bytes()).expect("Unable to write data");
        self.writer.write_all(b"\n").expect("Unable to write data");
    }

    fn flush(&mut self) {
        self.writer.flush().expect("Cannot flush buffer");
    }
}

#[derive(Debug)]
struct Parser {
    config: Config,
    info_per_table: HashMap<String, TableDataWriter>,
    schema_writer: io::BufWriter<File>,
}

impl Parser {
    fn new(config: Config, output_dir: &Path, schema_file: &PathBuf) -> Parser {
        Parser{
            config,
            info_per_table: HashMap::new(),
            schema_writer: get_writer(&PathBuf::from(output_dir).join(schema_file)),
        }
    }

    fn register_table(&mut self, table: &String) {
        self.info_per_table.insert(table.to_string(), TableDataWriter::new(table, &self.config));
    }

    // fn should_drop_statement(&self, reader::Statement { table: table_option, line, r#type: _ }: &reader::Statement) -> bool {
    //     let Some(table) = table_option else { return false };
    //     let Some(filters) = self.config.filter_per_table.get(table) else { return false };
    //
    //     for filter in filters.iter() {
    //         if !filter.has_determined_position() {
    //             let (_, fields) = reader::parse_fields(line.as_str()).unwrap();
    //             let field_position = fields.iter().position(|x| filter.matches_field(x));
    //             filter.set_position(&field_position);
    //         }
    //         dbg!(&filter);
    //     }
    //     false
    // }

    fn on_new_statement(&mut self, statement: &reader::Statement) {
        let reader::Statement { table: table_option, line, r#type: _ } = statement;
        // if self.should_drop_statement(&statement) {
        //     return
        // }
        match &table_option {
            None => {
                self.schema_writer.write_all(line.as_bytes()).expect("Unable to write data");
                self.schema_writer.write_all(b"\n").expect("Unable to write data");
            },
            Some(table) => {
                if !self.info_per_table.contains_key(table) {
                    self.register_table(table);
                }
                let info = self.info_per_table.get_mut(table).expect("Cannot find table info");
                info.on_new_statement(statement);
            },
        };
    }

    fn on_input_end(&mut self) {
        self.schema_writer.flush().expect("Unable to flush schema file");
        for info in self.info_per_table.values_mut() {
            info.flush();
        }
    }

    fn get_data_files(&mut self) -> Vec<PathBuf> {
        let filepaths: Vec<PathBuf> = self.info_per_table.values().map(|x| x.filepath.clone()).collect();
        filepaths
    }

    fn get_exported_tables(&mut self) -> HashSet<String> {
        let filepaths = HashSet::from_iter(self.info_per_table.keys().cloned());
        filepaths
    }
}

pub fn split(config: Config) -> (HashSet<String>, Vec<PathBuf>) {
    dbg!(&config);
    let mut table_info = Parser::new(config.clone(), &config.output_dir, &config.schema_file);
    for statement in reader::read_statements(&config.input_file, &config.requested_tables, true) {
        table_info.on_new_statement(&statement);
    }

    table_info.on_input_end();

    (table_info.get_exported_tables(), table_info.get_data_files())
}

// pub fn filter_inserts(sqldump_filepath: &PathBuf, field: &str, value: &str, output: &PathBuf) {
//     let lines = reader::read_lines(sqldump_filepath);
//     let mut writer: io::BufWriter<File> = get_writer(output);
//     let mut field_position: Option<usize> = None;
//
//     println!("Filtering table {} with {}={}", sqldump_filepath.display(), field, value);
//
//     let mut cond = FilterCondition {
//         field: field.to_owned(),
//         position: None,
//         operator: FilterOperator::Equals,
//         value: value.to_string(),
//     };
//
//     for line in lines.map_while(Result::ok) {
//         if !line.starts_with("INSERT INTO") {
//             writer.write_all(line.as_bytes()).expect("Unable to write data");
//             writer.write_all(b"\n").expect("Unable to write data");
//         } else {
//             if cond.position.is_none() {
//                 let (_, fields) = reader::parse_fields(line.as_str()).unwrap();
//                 let field_position = fields.iter().position(|x| x == &cond.field);
//                 cond.set_position(&field_position);
//             }
//
//             let (_, values) = reader::parse_values(cond.position.unwrap(), line.as_str()).unwrap();
//             let current_value = String::from(values.into_iter().nth(cond.position.unwrap()).unwrap());
//             cond.test(&current_value);
//
//
//
//             writer.write_all(line.as_bytes()).expect("Unable to write data");
//             writer.write_all(b"\n").expect("Unable to write data");
//         }
//     }
// }

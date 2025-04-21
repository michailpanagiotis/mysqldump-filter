use lazy_static::lazy_static;
use nom::multi::separated_list1;
use nom::{
  IResult,
  Parser,
  character::complete::{char, one_of, none_of},
  branch::alt,
  multi::separated_list0,
  bytes::complete::{escaped, is_not, take_until, tag, take_till},
  sequence::{delimited, preceded},
};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

use crate::config::TableConfig;
use crate::trackers::{InsertTracker, ReferenceTracker};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

#[derive(Debug)]
#[derive(Clone)]
pub struct FieldPositions(HashMap<String, usize>);

impl FieldPositions {
    fn new(insert_statement: &str) -> Self {
        let mut parser = preceded(
            take_until("("), preceded(take_until("`"), take_until(")"))
        ).and_then(
          separated_list0(
              tag(", "),
              delimited(char('`'), is_not("`"), char('`')),
          )
        );
        let res: IResult<&str, Vec<&str>> = parser.parse(insert_statement);
        let (_, fields) = res.expect("cannot parse fields");
        FieldPositions(HashMap::from_iter(
            fields.iter().enumerate().map(|(idx, item)| (item.to_string(), idx))
        ))
    }

    fn get_position(&self, field: &str) -> usize {
        self.0[field]
    }

    pub fn get_value(&self, statement: &Statement, field: &String) -> String {
        let values = statement.get_all_values();
        let position = self.0[field];
        values[position].clone()
    }

    pub fn get_values(&self, statement: &Statement, fields: &HashSet<String>) -> HashMap<String, String> {
        let values = statement.get_all_values();

        let value_per_field: HashMap<String, String> = HashMap::from_iter(fields.iter().map(|f| {
            let position = self.get_position(f);
            (f.clone(), values[position].clone())
        }));

        value_per_field
    }

    pub fn filtered(&mut self, fields: &HashSet<String>) -> Self {
        FieldPositions(HashMap::from_iter(
            self.0.iter()
                .filter(|(key, _)| fields.contains(*key))
                .map(|(key, value)| (key.clone(), *value))
        ))
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
#[derive(Clone)]
enum StatementType {
    Unknown,
    Insert,
}

#[derive(Debug)]
#[derive(Clone)]
pub struct Statement {
    line: String,
    table: Option<String>,
    r#type: StatementType,
}

impl Statement {
    pub fn new(table: &Option<String>, line: &str) -> Self {
       let statement_type = if line.starts_with("INSERT") { StatementType::Insert } else { StatementType::Unknown };
       Statement {
        line: line.to_string(),
        r#type: statement_type,
        table: table.clone(),
       }
    }

    pub fn from_file(sqldump_filepath: &Path, requested_tables: &HashSet<String>) -> impl Iterator<Item = Statement> + use<> {
        let mut current_table: Option<String> = None;
        let valid_tables = requested_tables.clone();
        let annotate_with_table = move |line: String| {
            if line.starts_with("-- Dumping data for table") {
                let table = TABLE_DUMP_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();
                current_table = Some(table);
            }
            if let Some(ref t) = current_table {
                if !valid_tables.contains(t) {
                    return None
                }
            }
            Some(Statement::new(&current_table, &line))
        };
        let file = File::open(sqldump_filepath).expect("Cannot open file");
        io::BufReader::new(file).lines()
            .map_while(Result::ok)
            .flat_map(annotate_with_table)
    }

    pub fn is_insert(&self) -> bool {
        self.r#type == StatementType::Insert
    }

    pub fn get_table(&self) -> Option<String> {
        self.table.clone()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.line.as_bytes()
    }

    pub fn get_field_positions(&self, fields: &HashSet<String>) -> Option<FieldPositions> {
        if !self.is_insert() {
            return None;
        }
        Some(FieldPositions::new(&self.line).filtered(fields))
    }

    pub fn get_all_values(&self) -> Vec<String> {
        let mut parser = preceded((take_until("VALUES ("), tag("VALUES (")), take_until(");")).and_then(
            separated_list1(
                one_of(",)"),
                alt((
                    // quoted value
                    delimited(
                        tag("'"),
                        // escaped or empty
                        alt((
                            escaped(none_of("\\\'"), '\\', tag("'")),
                            tag("")
                        )),
                        tag("'")
                    ),
                    // unquoted value
                    take_till(|c| c == ','),
                )),
            )
        );
        let res: IResult<&str, Vec<&str>> = parser.parse(&self.line);
        let (_, values) = res.expect("cannot parse values");
        values.iter().map(|item| item.to_string()).collect()
    }
}

pub struct TableStatementsIterator<'a, I: Iterator<Item=Statement>, F: Fn(&Statement) -> Option<String>> {
    inner: itertools::Group<'a, Option<String>, I, F>,
    insert_tracker: Option<InsertTracker>,
}

impl<I: Iterator<Item=Statement>, F: Fn(&Statement) -> Option<String>> Iterator for TableStatementsIterator<'_, I, F> {
    type Item = Statement;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.inner.next();
        while let Some(ref x) = next {
            if self.should_keep_item(x) {
                break;
            }
            next = self.inner.next();
        }

        next
    }
}

impl<'a, I: Iterator<Item=Statement>, F: Fn(&Statement) -> Option<String>> TableStatementsIterator<'a, I, F> {
    pub fn new(
        insert_tracker: Option<InsertTracker>,
        statements: itertools::Group<'a, Option<String>, I, F>,
    ) -> Self
    {
        TableStatementsIterator {
            inner: statements,
            insert_tracker,
        }
    }

    fn should_keep_item(&mut self, statement: &Statement) -> bool {
        if let Some(info) = &mut self.insert_tracker {
            info.should_keep_statement(statement)
        } else {
            true
        }
    }
}

pub struct TableStatements {
    pub table_config: TableConfig,
}

impl TableStatements {
    pub fn scan<I: Iterator<Item=Statement>, F: Fn(&Statement) -> Option<String>>(
        self,
        working_dir: &Path,
        default: &Path,
        statements: itertools::Group<Option<String>, I, F>,
    ) -> (Option<ReferenceTracker>, PathBuf) {
        let mut writer = self.table_config.get_writer(working_dir, default);
        let mut ref_tracker = self.table_config.get_reference_tracker();
        let insert_tracker = self.table_config.get_insert_tracker();

        if let Some(table) = &self.table_config.table {
            println!("Parsing table {}", &table);
        }

        for statement in TableStatementsIterator::new(insert_tracker, statements) {
            if let Some(ref mut tracker) = ref_tracker {
                tracker.capture(&statement);
            }
            writer.write_statement(&statement).expect("Unable to write data");
        }
        writer.flush().expect("Cannot flush buffer");
        (ref_tracker, writer.get_filepath())
    }
}

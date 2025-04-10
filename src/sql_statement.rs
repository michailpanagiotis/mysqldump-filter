use lazy_static::lazy_static;
use itertools::Itertools;
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
use std::path::Path;

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

    pub fn get_values(&self, statement: &Statement, fields: &[String]) -> HashMap<String, String> {
        let values = statement.get_all_values();

        let value_per_field: HashMap<String, String> = HashMap::from_iter(fields.iter().map(|f| {
            let position = self.get_position(f);
            (f.clone(), values[position].clone())
        }));

        value_per_field
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
    pub line: String,
    pub table: Option<String>,
    r#type: StatementType,
}

impl Statement {
    const SCHEMA_TABLE: &str = "INFORMATION_SCHEMA";

    pub fn new(table: &Option<String>, line: &str) -> Self {
       let statement_type = if line.starts_with("INSERT") { StatementType::Insert } else { StatementType::Unknown };
       Statement {
        line: line.to_string(),
        r#type: statement_type,
        table: table.clone(),
       }
    }

    pub fn from_file(sqldump_filepath: &Path) -> impl Iterator<Item = Statement> + use<> {
        let mut current_table: Option<String> = None;
        let annotate_with_table = move |line: String| {
            if line.starts_with("-- Dumping data for table") {
                let table = TABLE_DUMP_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();
                current_table = Some(table);
            }
            Statement::new(&current_table, &line)
        };
        let file = File::open(sqldump_filepath).expect("Cannot open file");
        io::BufReader::new(file).lines()
            .map_while(Result::ok)
            .map(annotate_with_table)

    }

    fn parse_lines<I: Iterator<Item=String>> (table: &String, lines: I) -> impl Iterator<Item = Statement> {
        lines.map(|l| Statement::new(&Some(table.clone()), l.as_str()))
    }

    fn get_table_from_line<'b, 'c>(line: &'b str, current_table: &'c mut Option<String>) -> String
        where 'c: 'b
    {
        if line.starts_with("-- Dumping data for table") {
            current_table.clone_from(
                &Some(TABLE_DUMP_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string())
            );
        }
        match current_table {
            Some(table) => table.to_string(),
            None => Statement::SCHEMA_TABLE.to_string(),
        }
    }

    // pub fn from_lines(statements: impl Iterator<Item=String> + 'static) -> impl IntoIterator<Item=(String, impl IntoIterator<Item=String>)>
    // {
    //     let mut current_table: Option<String> = None;
    //     let binding = statements.chunk_by(move |line| {
    //         Statement::get_table_from_line(&line, &mut current_table)
    //     }).into_iter();
    //
    //     let it = binding.into_iter();
    //
    //     it
    // }

    pub fn is_insert(&self) -> bool {
        self.r#type == StatementType::Insert
    }

    pub fn get_table(&self) -> Option<&String> {
        self.table.as_ref()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.line.as_bytes()
    }

    pub fn get_field_positions(&self) -> Option<FieldPositions> {
        if !self.is_insert() {
            return None;
        }
        Some(FieldPositions::new(&self.line))
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

    pub fn table_contained_in(&self, tables: &HashSet<String>) -> bool {
        if !self.is_insert() {
            return false;
        }
        tables.contains(self.table.as_ref().unwrap())
    }
}

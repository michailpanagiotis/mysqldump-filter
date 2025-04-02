use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use lazy_static::lazy_static;
use regex::Regex;
use fastbloom::BloomFilter;
use nom::{
  IResult,
  Parser,
  character::complete::char,
  branch::alt,
  multi::{separated_list0, many1, many_m_n},
  combinator::eof,
  bytes::complete::{is_not, take_until, tag},
  sequence::{delimited, preceded, terminated},
};

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
    static ref INSERT_RE: Regex = Regex::new(r"INSERT[^(]*\(([^)]+)\)").unwrap();
    static ref INSERT_VALUES_RE: Regex = Regex::new(r"INSERT.*\(([^)]+)\)").unwrap();
    static ref SPLIT_VALUES_RE: Regex = Regex::new(r"(?U)'[^']+'|[^,]+").unwrap();
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum StatementType {
    Unknown,
    Insert,
}

#[derive(Debug)]
pub struct Statement {
    pub line: String,
    pub table: Option<String>,
    pub r#type: StatementType,
}

impl Statement {
    pub fn is_insert(&self) -> bool {
        self.r#type == StatementType::Insert
    }

    pub fn as_str(&self) -> &str {
        &self.line
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.line.as_bytes()
    }

    pub fn get_field_positions(&self) -> Option<HashMap::<String, usize>> {
        if !self.is_insert() {
            return None;
        }
        let mut parser = preceded(
            take_until("("), preceded(take_until("`"), take_until(")"))
        ).and_then(
          separated_list0(
              tag(", "),
              delimited(char('`'), is_not("`"), char('`')),
          )
        );
        let res: IResult<&str, Vec<&str>> = parser.parse(&self.line);
        let (_, fields) = res.expect("cannot parse fields");
        Some(HashMap::from_iter(
            fields.iter().enumerate().map(|(idx, item)| (item.to_string(), idx))
        ))
    }

    pub fn get_values(&self) -> Vec<String> {
        let mut parser = preceded((take_until("VALUES ("), tag("VALUES (")), take_until(");")).and_then(
            // VALUES list
            many1(terminated(
                alt((
                    tag("''"),
                    // quoted value
                    delimited(char('\''), is_not("'"), char('\'')),
                    // unquoted value
                    take_until(",")
                )),
                // delimiter
                alt((tag(","), tag(","))),
            ))
        );
        let res: IResult<&str, Vec<&str>> = parser.parse(&self.line);
        let (_, values) = res.expect("cannot parse values");
        let values_vec: Vec<String> = values.iter().map(|item| item.to_string()).collect();
        dbg!(&values_vec);
        assert_eq!(values_vec.len(), 44);
        return values_vec;
    }
}

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
pub fn read_lines<P>(filename: P) -> io::Lines<io::BufReader<File>>
where P: AsRef<Path>, {
    let file = File::open(filename).expect("Cannot open file");
    io::BufReader::new(file).lines()
}

pub fn read_statements(sqldump_filepath: &PathBuf, requested_tables: &HashSet<String>, use_running_table: bool) -> impl Iterator<Item = Statement> {
    let mut current_table: Option<String> = None;
    let annotate_with_table = move |line: String| {
        if line.starts_with("-- Dumping data for table") {
            let table = TABLE_DUMP_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();
            current_table = Some(table);
        }
        let statement_type = if line.starts_with("INSERT") { StatementType::Insert } else { StatementType::Unknown };
        if !use_running_table {
            if let StatementType::Insert = statement_type {
                let table: String = line.chars().skip(13).take_while(|x| x != &'`').collect();
                current_table = Some(table);
            }
        }
        Statement { line, r#type: statement_type, table: current_table.clone() }
    };
    read_lines(sqldump_filepath)
        .map_while(Result::ok)
        .map(annotate_with_table)
        .filter(|st| st.table.is_none() || requested_tables.contains(st.table.as_ref().unwrap()))
}

pub fn parse_fields(input: &str) -> HashMap::<String, usize> {
    let res: IResult<&str, Vec<&str>> = preceded(
        take_until("("), preceded(take_until("`"), take_until(")"))
    ).and_then(
      separated_list0(
          tag(", "),
          delimited(char('`'), is_not("`"), char('`')),
      )
    ).parse(input);
    let (_, fields) = res.expect("cannot parse fields");
    HashMap::from_iter(
        fields.iter().enumerate().map(|(idx, item)| (item.to_string(), idx))
    )
}

pub fn parse_values(id_index: usize, input: &str) -> IResult<&str, Vec<&str>> {
    preceded((take_until("VALUES ("), tag("VALUES (")), take_until(");")).and_then(
        // VALUES list
        many_m_n(1, id_index + 1, terminated(
            alt((
                tag("''"),
                // quoted value
                delimited(char('\''), is_not("'"), char('\'')),
                // unquoted value
                take_until(",")
            )),
            // delimiter
            alt((tag(","), eof)),
        ))
    ).parse(input)
}

pub fn read_ids(filename: &String) -> (HashSet<String>, BloomFilter) {
    let lines = read_lines(filename);
    let mut id_position: Option<usize> = None;
    let mut ids: HashSet<String> = HashSet::new();
    println!("Reading ids of {}", filename);
    for line in lines.map_while(Result::ok) {
        if !line.starts_with("INSERT") {
            continue
        }

        if id_position.is_none() {
            let fields = parse_fields(line.as_str());
            id_position = fields.get("id").copied();
            if id_position.is_none() {
                id_position = fields.get("name").copied();
            }
        }

        let (_, values) = parse_values(id_position.unwrap(), line.as_str()).unwrap();
        let id = String::from(values.into_iter().nth(id_position.unwrap()).unwrap());
        ids.insert(id);
    }

    let ids_lookup = BloomFilter::with_false_pos(0.001).items(ids.iter());
    (ids, ids_lookup)
}

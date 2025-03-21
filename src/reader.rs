use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use lazy_static::lazy_static;
use regex::Regex;
use fastbloom::BloomFilter;
use nom::{
  IResult,
  Parser,
  sequence::delimited,
  sequence::preceded,
  sequence::terminated,
  bytes::complete::is_not,
  bytes::complete::take_until,
  bytes::complete::tag,
  multi::many_m_n,
  branch::alt,
  multi::separated_list0,
  combinator::eof,
};

lazy_static! {
    static ref INSERT_RE: Regex = Regex::new(r"INSERT[^(]*\(([^)]+)\)").unwrap();
    static ref INSERT_VALUES_RE: Regex = Regex::new(r"INSERT.*\(([^)]+)\)").unwrap();
    static ref SPLIT_VALUES_RE: Regex = Regex::new(r"(?U)'[^']+'|[^,]+").unwrap();
}

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
pub fn read_lines<P>(filename: P) -> io::Lines<io::BufReader<File>>
where P: AsRef<Path>, {
    let file = File::open(filename).expect("Cannot open file");
    io::BufReader::new(file).lines()
}


fn parse_fields(input: &str) -> IResult<&str, Vec<&str>> {
    preceded(take_until("("), preceded(take_until("`"), take_until(")"))).and_then(
      separated_list0(
          tag(", "),
          delimited(tag("`"), is_not("`"), tag("`")),
      )
    ).parse(input)
}

fn parse_values(id_index: usize, input: &str) -> IResult<&str, Vec<&str>> {
    preceded((take_until("VALUES ("), tag("VALUES (")), take_until(");")).and_then(
        // VALUES list
        many_m_n(1, id_index + 1, terminated(
            alt((
                tag("''"),
                // quoted value
                delimited(tag("'"), is_not("'"), tag("'")),
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
            let (_, fields) = parse_fields(line.as_str()).unwrap();
            id_position = fields.iter().position(|x| x == &"id");
            if id_position.is_none() {
                id_position = fields.iter().position(|x| x == &"name");
            }
        }

        let (_, values) = parse_values(id_position.unwrap(), line.as_str()).unwrap();
        let id = String::from(values.into_iter().nth(id_position.unwrap()).unwrap());
        ids.insert(id);
    }

    let ids_lookup = BloomFilter::with_false_pos(0.001).items(ids.iter());
    (ids, ids_lookup)
}

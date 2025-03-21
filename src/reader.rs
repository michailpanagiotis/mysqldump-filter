use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::path::Path;
use lazy_static::lazy_static;
use nom::AsChar;
use regex::Regex;
use fastbloom::BloomFilter;
use csv::{ByteRecord, ReaderBuilder, StringRecord};
use nom::{
  IResult,
  Parser,
  sequence::delimited,
  sequence::preceded,
  sequence::terminated,
  sequence::tuple,
  sequence::pair,
  sequence::separated_pair,
  // see the "streaming/complete" paragraph lower for an explanation of these submodules
  character::complete::char,
  bytes::complete::is_not,
  bytes::complete::take_while,
  bytes::complete::take_until,
  bytes::complete::take,
  bytes::complete::tag,
  bytes::complete::is_a,
  multi::many0,
  branch::alt,
  multi::separated_list0,
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

fn parse_id<T: FromStr>(id: &str) -> Option<T> {
    id.parse::<T>().ok()
}

fn is_not_cell_end(c: char) -> bool {
    c != ',' && c != '\n'
}

fn take4(input: &str) -> IResult<&str, &str> {
  take(4u8)(input)
}

fn quotes(input: &str) -> IResult<&str, &str> {
  delimited(char('\''), is_not("'"), char('\'')).parse(input)
}

fn abcd_parser(i: &str) -> IResult<&str, &str> {
  tag("abcd")(i) // will consume bytes if the input begins with "abcd"
}

fn parse_csv(input: &str) -> IResult<&str, &str> {
   alt((quotes, take_while(is_not_cell_end))).parse(input)
}

fn parse_insert(input: &str) -> IResult<&str, Vec<&str>> {
  delimited(
    terminated(is_not("("), tag("(")),
    is_not(")").and_then(
      separated_list0(
          tag(", "),
          delimited(tag("`"), is_not("`"), tag("`")),
      )
    ),
    tag(")")
  ).parse(input)
}


// fn parse_values(input: &str) -> IResult<&str, (&str, (&str, &str))> {
//     pair(
//       take_until(","),
//       is_not("\n").parse(input)
//     ).parse(input)
// }

fn parse_insert_statement(input: &str) -> IResult<&str, (Vec<&str>, (&str, &str))> {
    separated_pair(
        preceded(take_until("("), preceded(take_until("`"), take_until(")"))).and_then(
          separated_list0(
              tag(", "),
              delimited(tag("`"), is_not("`"), tag("`")),
          )
        ),
        take_until("("),
        preceded(tag("("), take_until(");")).and_then(
            (
                // terminated(take_until(","), alt((tag(","), is_not("\n")))),
                terminated(alt((delimited(tag("'"), is_not("'"), tag("'")), tag("NULL"), take_until(","))), alt((tag(","), is_not("\n")))),
                alt((delimited(tag("'"), is_not("'"), tag("'")), tag("NULL"), take_until(",")))
                // preceded(char('\''), take_until("',"))
                // take_until(",").and_then(preceded(char('\''), take_until("'"))),
            )
        )
    ).parse(input)
}

pub fn read_ids(filename: &String) -> (HashSet<i32>, BloomFilter) {
    let lines = read_lines(filename);
    let mut id_position: Option<usize> = None;
    let mut ids: HashSet<i32> = HashSet::new();
    println!("Reading ids of {}", filename);
    for line in lines.map_while(Result::ok) {
        if !line.starts_with("INSERT") {
            continue
        }

        if id_position.is_none() {
            let fields_string = INSERT_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();
            let mut fields = fields_string.split(", ");
            id_position = fields.position(|x| x.starts_with("`id`"));
        }

        let (leftover, parsed) = parse_insert_statement(line.as_str()).unwrap();

        let (_, values_str) = parsed;
        dbg!(values_str);

        // let (lef2, par2) = parse_values(values_str).unwrap();
        //
        // dbg!(par2);

        // println!("Reading table {}", line);
        // let values = INSERT_VALUES_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();
        // println!("{}", values);

        // let mut reader = ReaderBuilder::new()
        //     .has_headers(false)
        //         .delimiter(b',')
        //             .escape(Some(b'\\'))
        //             .double_quote(false)
        //             .quote(b'\\')
        //             .flexible(true)
        //             .quoting(false)
        //     .from_reader(values.as_bytes());
        // // dbg!(&reader);
        println!("NEW");

        // let res = parse_csv(values.as_str());
        // if res.is_ok() {
        //     println!("OK");
        //     dbg!(res.unwrap());
        //     // let (one, two) = res.unwrap();
        //     // println!("ONE {}", one);
        //     // println!("TWO {}", two);
        // } else {
        //     eprintln!("ERROR {}", res.err().unwrap());
        // }
        // for record in reader.records().map_while(Result::ok) {
        //     println!("REC");
        //     dbg!(&record);
        //     // println!(
        //     //     "In {}, {} built the {} model. It is a {}.",
        //     //     &record[0],
        //     //     &record[1],
        //     //     &record[2],
        //     //     &record[3]
        //     // );
        // }
        //
        // let mut count = 0;
        // dbg!(SPLIT_VALUES_RE.find_iter(
        //     values.as_str(),
        // ).map(
        //     |x| x.as_str(),
        // ).inspect(|_| count += 1).collect::<Vec<_>>());
        //
        // dbg!(count);
        //
        // let id: &str = SPLIT_VALUES_RE.find_iter(values.as_str())
        //     .map(|x| x.as_str())
        //     .nth(id_position.unwrap()).unwrap();
        // dbg!(id);
        // let parsed = parse_id::<i32>(id).unwrap();
        // ids.insert(parsed);
    }

    let ids_lookup = BloomFilter::with_false_pos(0.001).items(ids.iter());
    (ids, ids_lookup)
}

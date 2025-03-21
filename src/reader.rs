use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::path::Path;
use lazy_static::lazy_static;
use regex::Regex;
use fastbloom::BloomFilter;

lazy_static! {
    static ref INSERT_RE: Regex = Regex::new(r"INSERT[^(]*\(([^)]+)\)").unwrap();
    static ref INSERT_VALUES_RE: Regex = Regex::new(r"INSERT.*VALUES \((.*)\);").unwrap();
    static ref SPLIT_VALUES_RE: Regex = Regex::new(r"('[^']+')|([^,]*)").unwrap();
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

pub fn read_ids(filename: &String) {
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

        let values = INSERT_VALUES_RE.captures(&line).unwrap().get(1).unwrap().as_str().to_string();

        let id: &str = SPLIT_VALUES_RE.find_iter(values.as_str())
            .map(|x| x.as_str())
            .nth(id_position.unwrap()).unwrap();
        let parsed = parse_id::<i32>(id).unwrap();
        // ids.insert(parsed);
    }

    // let ids_lookup = BloomFilter::with_false_pos(0.001).items(ids.iter());
    // (ids, ids_lookup)
}

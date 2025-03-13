use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::env;
use regex::Regex;

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn main() {
    let re = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    dbg!(file_path);

    if let Ok(lines) = read_lines(file_path) {
        let mut table;
        // Consumes the iterator, returns an (Optional) String
        for line in lines.map_while(Result::ok) {
            if line.starts_with("-- Dumping data for table") {
                let caps = re.captures(&line).unwrap();
                table = caps.get(1).unwrap().as_str();
                println!("Reading table {}", table);
            }
        }
    }

}

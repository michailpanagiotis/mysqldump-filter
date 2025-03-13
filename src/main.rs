use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufWriter, Write};
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

fn get_write_buffer<P: AsRef<Path>>(filename: P) -> io::BufWriter<File> {
    File::create(&filename).expect("Unable to create file");
    let f = OpenOptions::new()
        .append(true)
        .open(&filename)
        .expect("Unable to open file");
    return BufWriter::new(f);
}

fn write_line(mut buffer: io::BufWriter<File>, line: &String) {
    buffer.write_all(line.as_bytes()).expect("Unable to write data");
    buffer.write_all(b"\n").expect("Unable to write data");
}

fn main() {
    let re = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    dbg!(file_path);

    if let Ok(lines) = read_lines(file_path) {
        let mut table;
        let mut buf = get_write_buffer("schema.sql");
        // Consumes the iterator, returns an (Optional) String
        for line in lines.map_while(Result::ok) {
            if line.starts_with("-- Dumping data for table") {
                let caps = re.captures(&line).unwrap();
                table = caps.get(1).unwrap().as_str();
                let filename = format!("{table}.sql");
                println!("Reading table {} into {}", table, filename);
                buf = get_write_buffer(&filename);
                write_line(buf, &line);
            }
        }
    }
}

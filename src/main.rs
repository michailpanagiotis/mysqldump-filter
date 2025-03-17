use lazy_static::lazy_static;
use regex::Regex;

mod options;
mod reader;

lazy_static! {
    static ref TABLE_DUMP_RE: Regex = Regex::new(r"-- Dumping data for table `([^`]*)`").unwrap();
}

fn get_table_name_from_comment(comment: &String) -> (String, String) {
    let caps = TABLE_DUMP_RE.captures(&comment).unwrap();
    let table = caps.get(1).unwrap().as_str().to_string();
    let filename = format!("{table}.sql");
    return (table, filename);
}

fn main() {
    let (all_tables, input_path) = options::parse_options();
    if let Ok(lines) = reader::read_lines(&input_path) {
        let mut current_table: Option<String> = None;
        for line in lines.map_while(Result::ok) {
            if line.starts_with("-- Dumping data for table") {
                let (table, filename) = get_table_name_from_comment(&line);
                println!("Reading table {} into {}", table, filename);
                // let file = tempfile().expect("Unable to open temporary file");
                // writers.insert(table.clone(), file);
                // wbuffers.insert(table.clone(), BufWriter::new(file));
                // dbg!(&wbuffers[&table]);

                // buf = get_write_buffer(&filename);
                current_table = Some(table.clone());
            }
            match current_table {
                Some(x) => {
                    let cloned = x.clone();
                    if all_tables.contains(&cloned) {

                    }
                },
                _ => ()
            }
            // buf.write_all(line.as_bytes()).expect("Unable to write data");
            // buf.write_all(b"\n").expect("Unable to write data");
        }
    }
}

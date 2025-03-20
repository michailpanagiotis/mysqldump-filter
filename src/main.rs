mod options;
mod reader;
mod splitter;

fn main() {
    let (input_path, requested_tables) = options::parse_options();
    let schema_file = String::from("schema.sql");
    let _exported_tables = splitter::split(&input_path, &schema_file, &requested_tables);
}

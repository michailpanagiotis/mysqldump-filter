use clap::Parser;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::io::{self, BufRead};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(value_name = "FILE", required=true)]
    input: PathBuf,
}

pub fn get_data_types(schema: &str) -> HashMap<String, sqlparser::ast::DataType> {
    let mut data_types = HashMap::new();

    let dialect = MySqlDialect {};
    let ast = SqlParser::parse_sql(&dialect, schema).unwrap();
    for st in ast.into_iter().filter(|x| matches!(x, sqlparser::ast::Statement::CreateTable(_))) {
        if let sqlparser::ast::Statement::CreateTable(ct) = st {
            for column in ct.columns.into_iter() {
                data_types.insert(ct.name.0[0].as_ident().unwrap().value.to_string() + "." + column.name.value.as_str(), column.data_type);
            }
        }
    }
    data_types
}

fn main() {
    let cli = Cli::parse();
    let input_file = std::env::current_dir().unwrap().to_path_buf().join(cli.input);

    let file = fs::File::open(input_file.as_path()).expect("Cannot open file");
    let lines = io::BufReader::new(file).lines().map_while(Result::ok);
    let sql: String = lines.filter(|l| !l.starts_with("--")).map(|l| l + "\n").collect();


    get_data_types(&sql);
}

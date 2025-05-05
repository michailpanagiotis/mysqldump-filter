use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::expression_parser::FilterCondition;

#[derive(Deserialize)]
#[serde(rename = "name")]
pub struct Config {
    pub allow_data_on_tables: HashSet<String>,
    filter_inserts: HashMap<String, Vec<String>>
}

impl Config {
    fn from_file(config_file: &Path) -> Self {
        let file = config::File::new(config_file.to_str().expect("invalid config path"), config::FileFormat::Json);
        let settings = config::Config::builder().add_source(file).build().expect("cannot read config file");
        settings.try_deserialize::<Config>().expect("malformed config")
    }

    pub fn get_conditions(&self, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Vec<FilterCondition> {
        self.filter_inserts.iter().flat_map(|(table, conditions)| conditions.iter().map(|c| FilterCondition::new(table, c, data_types))).collect()
    }
}

pub fn read_config(config_file: &Path) -> Config {
    let file = config::File::new(config_file.to_str().expect("invalid config path"), config::FileFormat::Json);
    let settings = config::Config::builder().add_source(file).build().expect("cannot read config file");
    settings.try_deserialize::<Config>().expect("malformed config")
}

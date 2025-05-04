use config::{Config, Environment, File, FileFormat};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::expression_parser::FilterCondition;

#[derive(Debug)]
pub struct Configuration {
    pub allowed_tables: HashSet<String>,
    filter_config: Vec<(String, String)>,
}

impl Configuration {
    pub fn new(settings: Config) -> Self {
        let allowed_tables: HashSet<_> = settings
            .get_array("allow_data_on_tables")
            .expect("no key 'allow_data_on_tables' in config")
            .iter().map(|x| x.to_string()).collect();

        let filter_inserts = settings
                .get_table("filter_inserts")
                .expect("no key 'filter_inserts' in config");

        let filter_config: Vec<_> = filter_inserts
            .iter()
            .flat_map(|(table, conditions)| {
                conditions.clone().into_array().expect("cannot parse config array").into_iter().map(move |x| {
                    (table.clone(), x.to_string())
                })
            }).collect();

        Configuration {
            allowed_tables,
            filter_config,
        }
    }

    pub fn get_conditions(&self, data_types: &HashMap<String, sqlparser::ast::DataType>) -> Vec<FilterCondition> {
        self.filter_config.iter().map(|(table, definition)| FilterCondition::new(table, definition, data_types)).collect()
    }
}

impl From<&Path> for Configuration {
    fn from(config_file: &Path) -> Self {
        let settings = Config::builder()
            .add_source(File::new(config_file.to_str().expect("invalid config path"), FileFormat::Json))
            .add_source(Environment::with_prefix("MYSQLDUMP_FILTER"))
            .build()
            .unwrap();

        Configuration::new(settings)
    }
}

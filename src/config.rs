use std::collections::{HashSet, HashMap};

#[derive(Debug)]
#[derive(Clone)]
pub struct Config {
    pub requested_tables: HashSet<String>,
    pub filter_per_table: HashMap<String, Vec<String>>,
}

pub fn parse(config_file: &str) -> Config {
    let settings = config::Config::builder()
        .add_source(config::File::new(config_file, config::FileFormat::Json))
        .add_source(config::Environment::with_prefix("MYSQLDUMP_FILTER"))
        .build()
        .unwrap();
    let requested_tables: HashSet<_> = settings
        .get_array("allow_data_on_tables")
        .expect("no key 'allow_data_on_tables' in config")
        .iter().map(|x| x.to_string()).collect();

    let filter_per_table: HashMap<String, Vec<String>>= settings
        .get_table("filter_inserts")
        .expect("no key 'filter_inserts' in config")
        .into_iter()
        .map(|(key, value)| (
            key,
            value
                .into_array()
                .expect("invalid value")
                .into_iter()
                .map(|x| x.to_string())
                .collect())
        )
        .collect();
    dbg!(&filter_per_table);
    Config {
        requested_tables,
        filter_per_table,
    }
}

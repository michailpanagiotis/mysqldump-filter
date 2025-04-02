use nom::{
  IResult,
  Parser,
  bytes::complete::{is_not, tag},
  branch::alt,
};
use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Config {
    pub working_dir_path: PathBuf,
    pub schema_file: PathBuf,
    pub requested_tables: HashSet<String>,
    pub filter_per_table: HashMap<String, Vec<FilterCondition>>,
}

impl Config {
    pub fn new(
        config_file: &Path,
        working_dir_path: &Path,
    ) -> Config {
        let settings = config::Config::builder()
            .add_source(config::File::new(config_file.to_str().expect("invalid config path"), config::FileFormat::Json))
            .add_source(config::Environment::with_prefix("MYSQLDUMP_FILTER"))
            .build()
            .unwrap();
        let requested_tables: HashSet<_> = settings
            .get_array("allow_data_on_tables")
            .expect("no key 'allow_data_on_tables' in config")
            .iter().map(|x| x.to_string()).collect();

        let filter_per_table: HashMap<String, Vec<FilterCondition>>= settings
            .get_table("filter_inserts")
            .expect("no key 'filter_inserts' in config")
            .into_iter()
            .map(|(key, value)| (
                key,
                value
                    .into_array()
                    .expect("invalid value")
                    .into_iter()
                    .map(|x| FilterCondition::new(x.to_string()))
                    .collect())
            )
            .collect();
        let schema_file = working_dir_path.join("schema.sql");
        Config {
            schema_file: schema_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            filter_per_table,
        }
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    Unknown,
}

#[derive(Debug)]
#[derive(Clone)]
pub struct FilterCondition {
    pub field: String,
    operator: FilterOperator,
    value: String,
}

impl FilterCondition {
    fn parse_query(input: &str) -> IResult<&str, (&str, &str, &str)> {
        (
            is_not("!="),
            alt((tag("=="), tag("!="))),
            is_not("=")
        ).parse(input)
    }

    fn new(definition: String) -> FilterCondition {
        let (_, parsed) = FilterCondition::parse_query(&definition).expect("cannot parse filter condition");
        let (field, operator, value) = parsed;
        FilterCondition {
            field: field.to_string(),
            operator: match operator {
                "==" => FilterOperator::Equals,
                "!=" => FilterOperator::NotEquals,
                _ => FilterOperator::Unknown,
            },
            value: value.to_string(),
        }
    }

    pub fn test(&self, other_value: &String) -> bool {
        match &self.operator {
            FilterOperator::Equals => &self.value == other_value,
            FilterOperator::NotEquals => &self.value != other_value,
            FilterOperator::Unknown => false
        }
    }
}

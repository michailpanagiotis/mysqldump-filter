use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};
use std::usize;
use nom::{
  IResult,
  Parser,
  bytes::complete::{is_not, tag},
  branch::alt,
};

#[derive(Debug)]
#[derive(Clone)]
pub struct Config {
    pub input_file: PathBuf,
    pub output_dir: PathBuf,
    pub requested_tables: HashSet<String>,
    pub filter_per_table: HashMap<String, Vec<FilterCondition>>,
    pub schema_file: PathBuf,
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
    field: String,
    position: Option<usize>,
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
            position: None,
            operator: match operator {
                "==" => FilterOperator::Equals,
                "!=" => FilterOperator::NotEquals,
                _ => FilterOperator::Unknown,
            },
            value: value.to_string(),
        }
    }

    fn test(&self, other_value: &String) -> bool {
        match &self.operator {
            FilterOperator::Equals => &self.value == other_value,
            FilterOperator::NotEquals => &self.value != other_value,
            FilterOperator::Unknown => false
        }
    }

    pub fn matches_field(&self, field_name: &str) -> bool {
        return &self.field == field_name;
    }

    pub fn has_determined_position(&self) -> bool {
        self.position.is_some()
    }

    pub fn set_position(&mut self, position: &Option<usize>) {
        self.position = *position;
    }
}

pub fn parse(config_file: &str, input_file: &PathBuf, output_dir: &Path, schema_file: &PathBuf) -> Config {
    let settings = config::Config::builder()
        .add_source(config::File::new(config_file, config::FileFormat::Json))
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
    Config {
        output_dir: output_dir.to_path_buf(),
        input_file: input_file.clone(),
        schema_file: schema_file.clone(),
        requested_tables,
        filter_per_table,
    }
}

use itertools::Itertools;
use nom::{
  IResult,
  Parser,
  bytes::complete::{is_not, tag},
  branch::alt,
  combinator::rest,
};
use std::{collections::{HashMap, HashSet}, iter::Filter};
use std::path::{Path, PathBuf};

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    References,
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
    fn new(definition: String) -> FilterCondition {
        let mut parser = (
            is_not("!=-"),
            alt((tag("=="), tag("!="), tag("->"))),
            rest
        );
        let res: IResult<&str, (&str, &str, &str)> = parser.parse(&definition);
        let (_, parsed) = res.expect("cannot parse filter condition");
        let (field, operator, value) = parsed;
        FilterCondition {
            field: field.to_string(),
            operator: match operator {
                "==" => FilterOperator::Equals,
                "!=" => FilterOperator::NotEquals,
                "->" => FilterOperator::References,
                _ => FilterOperator::Unknown,
            },
            value: value.to_string(),
        }
    }

    fn from_value(value: config::Value) -> Self {
        FilterCondition::new(value.to_string())
    }

    pub fn test(&self, other_value: &String) -> bool {
        match &self.operator {
            FilterOperator::Equals => &self.value == other_value,
            FilterOperator::NotEquals => &self.value != other_value,
            FilterOperator::References => true,
            FilterOperator::Unknown => true
        }
    }

    pub fn is_reference(&self) -> bool {
        self.operator == FilterOperator::References
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct Filters {
    items: Vec<FilterCondition>,
}

impl Filters {
    pub fn from_value(value: &config::Value) -> Self {
        Filters {
            items: value.clone().into_array().unwrap().into_iter().map(FilterCondition::from_value).collect()
        }
    }

    pub fn to_direct_filters(&self) -> Self {
        Filters {
            items: self.items.clone()
                .into_iter()
                .filter(|x| !x.is_reference())
                .collect()
        }
    }
}

#[derive(Debug)]
pub struct Config {
    pub working_dir_path: PathBuf,
    pub schema_file: PathBuf,
    pub requested_tables: HashSet<String>,
    pub direct_filters_per_table: HashMap<String, Vec<FilterCondition>>,
    pub filters_per_table: HashMap<String, Vec<FilterCondition>>,
    pub references_per_table: HashMap<String, Vec<String>>,
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

        let filters_per_table: HashMap<String, Vec<FilterCondition>>= settings
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

        let references_per_table: HashMap<String, Vec<String>> = filters_per_table
            .values()
            .flatten()
            .filter(|x| x.is_reference())
            .map(|x| x.value.clone())
            .unique()
            .map(|x| {
                let parts: Vec<&str> = x.split(".").collect();
                (parts[0].to_string(), parts[1].to_string())
            })
            .into_group_map();

        let res: HashMap<String, Filters> = settings.get_table("filter_inserts")
            .expect("no key 'filter_inserts' in config")
            .iter().map(|(k, v)| (k.to_string(), Filters::from_value(v))).collect();
        dbg!(&res);

        let direct_filters_per_table: HashMap<String, Vec<FilterCondition>> = HashMap::from_iter(filters_per_table.clone().into_iter().map(|(k, v)| {
            (k, v.into_iter().filter(|x| !x.is_reference()).collect())
        }));

        let schema_file = working_dir_path.join("schema.sql");
        Config {
            schema_file: schema_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            filters_per_table,
            direct_filters_per_table,
            references_per_table,
        }
    }
}

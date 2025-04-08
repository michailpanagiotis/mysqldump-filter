use itertools::Itertools;
use nom::{
  IResult,
  Parser,
  bytes::complete::{is_not, tag},
  branch::alt,
  combinator::rest,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq)]
enum FilterOperator {
    Equals,
    NotEquals,
    References,
    Unknown,
}

#[derive(Debug)]
#[derive(Clone)]
struct FilterCondition {
    field: String,
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

    fn from_config_value(value: config::Value) -> Self {
        FilterCondition::new(value.to_string())
    }

    fn test(&self, other_value: &str) -> bool {
        match &self.operator {
            FilterOperator::Equals => self.value == other_value,
            FilterOperator::NotEquals => self.value != other_value,
            FilterOperator::References => true,
            FilterOperator::Unknown => true
        }
    }

    fn is_reference(&self) -> bool {
        self.operator == FilterOperator::References
    }
}


#[derive(Debug)]
#[derive(Clone)]
pub struct TableFilters(HashMap<String, Vec<FilterCondition>>);

impl TableFilters {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get_filtered_fields(&self) -> Vec<String> {
        self.0.keys().cloned().collect()
    }

    pub fn test(&self, value_per_field: HashMap<String, String>) -> bool {
        for (field, value) in value_per_field {
            if !self.test_single_field(&field, &value) {
                return false;
            }
        }
        true
    }

    fn from_conditions(conditions: &[FilterCondition]) -> Self {
        let res: HashMap<String, Vec<FilterCondition>> = conditions.iter().map(|cond| {
            (cond.field.clone(), cond.to_owned())
        }).into_group_map();

        TableFilters(res)
    }

    fn from_config_value(value: &config::Value) -> Self {
        let conditions: Vec<FilterCondition> = value.clone().into_array().unwrap().into_iter().map(FilterCondition::from_config_value).collect();
        TableFilters::from_conditions(&conditions)
    }

    fn get_conditions(&self) -> Vec<FilterCondition> {
        self.0.values().flatten().cloned().collect()
    }

    fn empty() -> Self {
        TableFilters(HashMap::new())
    }

    fn test_single_field(&self, field: &str, value: &str) -> bool {
        let Some(conditions) = self.0.get(field) else { return true };

        for condition in conditions {
            if !condition.test(value) {
                return false;
            }
        }
        true
    }

    fn to_direct_filters(&self) -> Self {
        let conditions: Vec<FilterCondition> = self.get_conditions().iter().filter(|x| !x.is_reference()).cloned().collect();
        TableFilters::from_conditions(&conditions)
    }

    fn to_reference_filters(&self) -> Self {
        let conditions: Vec<FilterCondition> = self.get_conditions().iter().filter(|x| x.is_reference()).cloned().collect();
        TableFilters::from_conditions(&conditions)
    }
}

#[derive(Debug)]
pub struct FilterMap(HashMap<String, TableFilters>);

impl FilterMap {
    fn from_config_value(value: &HashMap<String, config::Value>) -> Self {
        let res: HashMap<String, TableFilters> = value.iter()
            .map(|(k, v)| (k.clone(), TableFilters::from_config_value(v)))
            .filter(|(_, v)| !v.is_empty())
            .collect();
        FilterMap(res)
    }

    fn to_direct_filters(&self) -> Self {
        let res: HashMap<String, TableFilters> = self.0.iter()
            .map(|(k, v)| (k.clone(), v.to_direct_filters()))
            .filter(|(_, v)| !v.is_empty())
            .collect();
        FilterMap(res)
    }

    fn to_reference_filters(&self) -> Self {
        let res: HashMap<String, TableFilters> = self.0.iter()
            .map(|(k, v)| (k.clone(), v.to_reference_filters()))
            .filter(|(_, v)| !v.is_empty())
            .collect();
        FilterMap(res)
    }

    pub fn get(&self, key: &str) -> TableFilters {
        match self.0.get(key) {
            Some(x) => {
                x.clone()
            },
            None => TableFilters::empty()
        }
    }
}

#[derive(Debug)]
pub struct Config {
    pub working_dir_path: PathBuf,
    pub schema_file: PathBuf,
    pub requested_tables: HashSet<String>,
    pub direct_filters_per_table: FilterMap,
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

        let res = FilterMap::from_config_value(
            &settings.get_table("filter_inserts").expect("no key 'filter_inserts' in config"),
        );
        dbg!(&res);

        let direct_filters_per_table = res.to_direct_filters();
        dbg!(&direct_filters_per_table);

        let reference_filters_per_table = res.to_reference_filters();
        dbg!(&reference_filters_per_table);

        let schema_file = working_dir_path.join("schema.sql");
        Config {
            schema_file: schema_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            direct_filters_per_table,
            references_per_table,
        }
    }
}

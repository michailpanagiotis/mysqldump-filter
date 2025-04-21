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

    fn get_reference_parts(&self) -> (String, String) {
        let parts: Vec<&str> = self.value.split(".").collect();
        (parts[0].to_string(), parts[1].to_string())
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct TableFilters {
    filtered_fields: HashSet<String>,
    filters_per_field: HashMap<String, Vec<FilterCondition>>,
}

impl TableFilters {
    pub fn is_empty(&self) -> bool {
        self.filters_per_field.is_empty()
    }

    pub fn get_filtered_fields(&self) -> &HashSet<String> {
        &self.filtered_fields
    }

    pub fn test(&self, value_per_field: &HashMap<String, String>) -> bool {
        for (field, value) in value_per_field {
            if !self.test_single_field(field, value) {
                return false;
            }
        }
        true
    }

    fn from_conditions(conditions: &[FilterCondition]) -> Self {
        let res: HashMap<String, Vec<FilterCondition>> = conditions.iter().map(|cond| {
            (cond.field.clone(), cond.to_owned())
        }).into_group_map();

        let filtered_fields = res.keys().cloned().collect();
        TableFilters{ filtered_fields, filters_per_field: res }
    }

    fn from_config_value(value: &config::Value) -> Self {
        let conditions: Vec<FilterCondition> = value.clone().into_array().unwrap()
            .into_iter()
            .map(FilterCondition::from_config_value).collect();
        TableFilters::from_conditions(&conditions)
    }

    fn get_direct_conditions(&self) -> Vec<FilterCondition> {
        self.filters_per_field.values().flatten().filter(|x| !x.is_reference()).cloned().collect()
    }

    fn get_reference_conditions(&self) -> Vec<FilterCondition> {
        self.filters_per_field.values().flatten().filter(|x| x.is_reference()).cloned().collect()
    }

    fn get_references(&self) -> Vec<(String, String)> {
        self.get_reference_conditions().iter().map(|x| x.get_reference_parts()).collect()
    }

    pub fn empty() -> Self {
        TableFilters{ filtered_fields: HashSet::new(), filters_per_field: HashMap::new(),  }
    }

    fn test_single_field(&self, field: &str, value: &str) -> bool {
        let Some(conditions) = self.filters_per_field.get(field) else { return true };

        for condition in conditions {
            if !condition.test(value) {
                return false;
            }
        }
        true
    }

    pub fn to_direct_filters(&self) -> Self {
        let conditions: Vec<FilterCondition> = self.get_direct_conditions();
        TableFilters::from_conditions(&conditions)
    }

    pub fn to_reference_filters(&self) -> Self {
        let conditions: Vec<FilterCondition> = self.get_reference_conditions();
        TableFilters::from_conditions(&conditions)
    }
}

#[derive(Debug)]
pub struct FilterMap(HashMap<String, TableFilters>);

impl FilterMap {
    fn from_iter(iter: impl Iterator<Item=(String, TableFilters)>) -> Self {
        let res: HashMap<String, TableFilters> = iter
            .filter(|(_, v)| !v.is_empty())
            .collect();
        FilterMap(res)
    }

    fn from_config_value(value: &HashMap<String, config::Value>) -> Self {
        FilterMap::from_iter(
            value.iter().map(|(k, v)| (k.clone(), TableFilters::from_config_value(v)))
        )
    }

    fn get_references(&self) -> HashMap<String, Vec<String>> {
        self.0.values()
            .flat_map(|v| v.get_references())
            .unique()
            .into_group_map()
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
    pub filters_per_table: FilterMap,
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

        let filters_per_table = FilterMap::from_config_value(
            &settings.get_table("filter_inserts").expect("no key 'filter_inserts' in config"),
        );

        let references_per_table = filters_per_table.get_references();

        let schema_file = working_dir_path.join("schema.sql");
        Config {
            schema_file: schema_file.to_path_buf(),
            working_dir_path: working_dir_path.to_path_buf(),
            requested_tables,
            filters_per_table,
            references_per_table,
        }
    }

    pub fn get_filters(&self, table: &Option<String>) -> Option<TableFilters> {
        table.as_ref().map(|t| self.filters_per_table.get(t))
    }

    pub fn get_referenced_fields(&self, table: &Option<String>) -> HashSet<String> {
        match table {
            None => HashSet::new(),
            Some(t) => {
                match self.references_per_table.get(t) {
                    Some(x) => HashSet::from_iter(x.iter().cloned()),
                    None => HashSet::new(),
                }
            }
        }
    }
}

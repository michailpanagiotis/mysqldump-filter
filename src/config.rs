use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::io_utils::SQLWriter;
use crate::parser::parse_filter;
use crate::trackers::{InsertTracker, ReferenceTracker};
use crate::sql_statement::{Statement, TableStatementsIterator};

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
        let (field, operator, value) = parse_filter(definition.as_str());
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
pub struct FieldFilters {
    table: String,
    field: String,
    conditions: Vec<FilterCondition>,
}

impl FieldFilters {
    fn test_value(&self, value: &str) -> bool {
        self.conditions.iter().filter(|x| !x.is_reference()).all(|condition| condition.test(value))
    }

    fn test_reference(&self, value: &str, references: &HashMap<String, HashSet<String>>) -> bool {
        self.conditions.iter().filter(|x| x.is_reference()).all(|condition| {
            let Some(set) = references.get(condition.value.as_str()) else { return false };
            set.contains(value)
        })
    }

    fn get_direct_conditions(&self) -> Vec<FilterCondition> {
        self.conditions.iter().filter(|x| !x.is_reference()).cloned().collect()
    }

    fn get_reference_conditions(&self) -> Vec<FilterCondition> {
        self.conditions.iter().filter(|x| x.is_reference()).cloned().collect()
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct TableFilters {
    table: String,
    filtered_fields: HashSet<String>,
    per_field: HashMap<String, FieldFilters>,
}

impl TableFilters {
    pub fn is_empty(&self) -> bool {
        self.per_field.is_empty()
    }

    pub fn get_filtered_fields(&self) -> &HashSet<String> {
        &self.filtered_fields
    }

    pub fn test_values(&self, value_per_field: &HashMap<String, String>) -> bool {
        self.per_field.iter().all(|(field, field_filters)| {
            value_per_field.get(field).is_some_and(|v| field_filters.test_value(v))
        })
    }

    pub fn test_values_against_references(&self, value_per_field: &HashMap<String, String>, references: &HashMap<String, HashSet<String>>) -> bool {
        self.per_field.iter().all(|(field, field_filters)| {
            value_per_field.get(field).is_some_and(|v| field_filters.test_reference(v, references))
        })
    }

    fn from_conditions(table: &String, conditions: &[FilterCondition]) -> Self {
        let res: HashMap<String, Vec<FilterCondition>> = conditions.iter().map(|cond| {
            (cond.field.clone(), cond.to_owned())
        }).into_group_map();

        let res2: HashMap<String, FieldFilters> = HashMap::from_iter(res.iter().map(|(field, value)| (field.clone(), FieldFilters {
            table: table.clone(),
            field: field.clone(),
            conditions: value.clone(),
        })));

        let filtered_fields = res.keys().cloned().collect();
        TableFilters{ table: table.clone(), filtered_fields, per_field: res2 }
    }

    fn from_config_value(table: &String, value: &config::Value) -> Self {
        let conditions: Vec<FilterCondition> = value.clone().into_array().unwrap()
            .into_iter()
            .map(FilterCondition::from_config_value).collect();
        TableFilters::from_conditions(&table, &conditions)
    }

    fn get_direct_conditions(&self) -> Vec<FilterCondition> {
        self.per_field.values().flat_map(|x| x.get_direct_conditions()).collect()
    }

    fn get_reference_conditions(&self) -> Vec<FilterCondition> {
        self.per_field.values().flat_map(|x| x.get_reference_conditions()).collect()
    }

    fn get_references(&self) -> Vec<(String, String)> {
        self.get_reference_conditions().iter().map(|x| x.get_reference_parts()).collect()
    }

    pub fn empty(table: &str) -> Self {
        TableFilters{ table: table.to_string(), filtered_fields: HashSet::new(), per_field: HashMap::new()  }
    }

    pub fn to_direct_filters(&self) -> Self {
        let conditions: Vec<FilterCondition> = self.get_direct_conditions();
        TableFilters::from_conditions(&self.table, &conditions)
    }

    pub fn to_reference_filters(&self) -> Self {
        let conditions: Vec<FilterCondition> = self.get_reference_conditions();
        TableFilters::from_conditions(&self.table, &conditions)
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
            value.iter().map(|(k, v)| (k.clone(), TableFilters::from_config_value(k, v)))
        )
    }

    fn get_references(&self) -> HashMap<String, Vec<String>> {
        self.0.values()
            .flat_map(|v| v.get_references())
            .unique()
            .into_group_map()
    }

    pub fn get(&self, key: &str) -> Option<TableFilters> {
        self.0.get(key).cloned()
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
        let Some(t) = table else { return None };
        self.filters_per_table.get(t)
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

    pub fn get_table_config(&self, table: &Option<String>) -> TableConfig {
        let referenced_fields = &self.get_referenced_fields(table);
        let filters = &self.get_filters(table);
        TableConfig::new(&self.working_dir_path, &self.schema_file, table, filters, referenced_fields)
    }

    pub fn read_statements(&self, input_file: &Path) -> impl Iterator<Item=Statement> {
        Statement::from_file(input_file, &self.requested_tables)
    }
}

#[derive(Debug)]
pub struct TableConfig {
    working_dir: PathBuf,
    default_file: PathBuf,
    table: Option<String>,
    filters: Option<TableFilters>,
    referenced_fields: HashSet<String>,
}

impl TableConfig {
    pub fn new(
        working_dir: &Path,
        default_file: &Path,
        table: &Option<String>,
        filters: &Option<TableFilters>,
        referenced_fields: &HashSet<String>,
    ) -> TableConfig
    {
        TableConfig {
            working_dir: working_dir.to_path_buf(),
            default_file: default_file.to_path_buf(),
            table: table.clone(),
            filters: filters.clone(),
            referenced_fields: referenced_fields.clone(),
        }
    }

    pub fn get_writer(&self) -> SQLWriter {
        SQLWriter::new( &self.table, &self.working_dir, &self.default_file)
    }

    pub fn get_table(&self) -> &Option<String> {
        &self.table
    }

    fn get_insert_tracker<'a>(&self, references: Option<&'a HashMap<String, HashSet<String>>>) -> Option<InsertTracker<'a>> {
        self.table.clone().map(|t| InsertTracker::new(
            &t,
            &self.filters,
            references,
        ))
    }

    pub fn get_reference_tracker(&self) -> Option<ReferenceTracker> {
        let ref_tracker = match self.table.is_some() && !self.referenced_fields.is_empty() {
            true => Some(ReferenceTracker::new(self.table.as_ref().unwrap(), &self.referenced_fields)),
            false => None,
        };
        ref_tracker
    }

    pub fn filter_statements<I: Iterator<Item=Statement>>(
        &self,
        statements: I,
        references: Option<&HashMap<String, HashSet<String>>>,
    ) -> impl Iterator<Item=Statement> {
        TableStatementsIterator::new(self.get_insert_tracker(references), statements)
    }
}

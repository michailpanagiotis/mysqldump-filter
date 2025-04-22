use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::io_utils::SQLWriter;
use crate::expression_parser::parse_filter;
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
    table: String,
    field: String,
    operator: FilterOperator,
    value: String,
}

impl FilterCondition {
    fn new(table: &str, definition: &str) -> FilterCondition {
        let (field, operator, value) = parse_filter(definition);
        FilterCondition {
            table: table.to_string(),
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
#[derive(Default)]
#[derive(Clone)]
pub struct FieldFilters {
    table: String,
    field: String,
    conditions: Vec<FilterCondition>,
}

impl Extend<FilterCondition> for FieldFilters {
    fn extend<T>(&mut self, conditions: T)
       where T: IntoIterator<Item = FilterCondition> {
        self.conditions.extend(conditions);
        self.assert_sane();
    }
}

impl FromIterator<FilterCondition> for FieldFilters {
    fn from_iter<T>(iter: T) -> Self
       where T: IntoIterator<Item = FilterCondition> {
        let conditions: Vec<FilterCondition> = iter.into_iter().collect();

        let instance = FieldFilters {
            table: conditions[0].table.clone(),
            field: conditions[0].field.clone(),
            conditions,
        };
        instance.assert_sane();
        instance
    }
}

impl FieldFilters {
    fn assert_sane(&self) {
        let distinct: Vec<&FilterCondition> = self.conditions.iter().unique_by(|s| (&s.table, &s.field)).collect();
        if distinct.len() > 1 {
            panic!("conditions have different fields");
        }
    }

    fn test_value(&self, value: &str) -> bool {
        self.conditions.iter().filter(|x| !x.is_reference()).all(|condition| condition.test(value))
    }

    fn test_reference(&self, value: &str, references: &HashMap<String, HashSet<String>>) -> bool {
        self.conditions.iter().filter(|x| x.is_reference()).all(|condition| {
            let Some(set) = references.get(condition.value.as_str()) else { return false };
            set.contains(value)
        })
    }

    fn get_references(&self) -> Vec<(String, String)> {
        self.conditions.iter().filter(|x| x.is_reference()).cloned().map(|x| x.get_reference_parts()).collect()
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct TableFilters {
    table: String,
    per_field: HashMap<String, FieldFilters>,
}

impl TableFilters {
    fn new<I: Iterator<Item=String>>(table: &str, conditions: I) -> Self {
        let res: HashMap<String, FieldFilters> = conditions.map(|ref x| {
            let cond = FilterCondition::new(table, x);
            (cond.field.clone(), cond)
        }).into_grouping_map().collect();

        TableFilters{ table: table.to_string(), per_field: res }
    }

    pub fn is_empty(&self) -> bool {
        self.per_field.is_empty()
    }

    pub fn empty(table: &str) -> Self {
        TableFilters{ table: table.to_string(), per_field: HashMap::new()  }
    }

    pub fn get_filtered_fields(&self) -> HashSet<String> {
        self.per_field.values().map(|x| x.field.clone()).collect()
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

    fn get_references(&self) -> Vec<(String, String)> {
        self.per_field.values().flat_map(|v| v.get_references()).collect()
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
            value.iter().map(|(table, conditions)| {
                let config_conditions = conditions.clone().into_array().expect("cannot parse config array").into_iter().map(|x| x.to_string());
                (table.clone(), TableFilters::new(table, config_conditions))
            })
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

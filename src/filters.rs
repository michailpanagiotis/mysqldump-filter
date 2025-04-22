use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::parse_filter;

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
    fn extend<T: IntoIterator<Item = FilterCondition>>(&mut self, conditions: T) {
        let other = FieldFilters::from_iter(conditions);
        if other.table != self.table || other.field != self.field {
            panic!("filter conditions have different fields");
        }
        self.conditions.extend(other.conditions);
    }
}

impl FromIterator<FilterCondition> for FieldFilters {
    fn from_iter<T: IntoIterator<Item = FilterCondition>>(iter: T) -> Self {
        let conditions: Vec<FilterCondition> = iter.into_iter().collect();

        let distinct: Vec<&FilterCondition> = conditions.iter().unique_by(|s| (&s.table, &s.field)).collect();
        if distinct.len() > 1 {
            panic!("conditions have different fields");
        }

        FieldFilters {
            table: conditions[0].table.clone(),
            field: conditions[0].field.clone(),
            conditions,
        }
    }
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

    fn get_references(&self) -> Vec<(String, String)> {
        self.conditions.iter().filter(|x| x.is_reference()).cloned().map(|x| x.get_reference_parts()).collect()
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct TableFilters {
    per_field: HashMap<String, FieldFilters>,
}

impl TableFilters {
    pub fn new<I: Iterator<Item=String>>(table: &str, conditions: I) -> Self {
        let res: HashMap<String, FieldFilters> = conditions.map(|ref x| {
            let cond = FilterCondition::new(table, x);
            (cond.field.clone(), cond)
        }).into_grouping_map().collect();

        TableFilters{ per_field: res }
    }

    pub fn is_empty(&self) -> bool {
        self.per_field.is_empty()
    }

    pub fn empty() -> Self {
        TableFilters{ per_field: HashMap::new()  }
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

    pub fn get_references(&self) -> Vec<(String, String)> {
        self.per_field.values().flat_map(|v| v.get_references()).collect()
    }
}

impl FromIterator<FilterCondition> for TableFilters {
    fn from_iter<T: IntoIterator<Item = FilterCondition>>(iter: T) -> Self {
        TableFilters {
            per_field: iter.into_iter().chunk_by(|x| x.field.clone()).into_iter().map(|(field, items)| (field, FieldFilters::from_iter(items))).collect(),
        }
    }
}

use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::{parse_filter, parse_insert_fields, parse_insert_values};

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
pub struct FilterCondition {
    table: String,
    field: String,
    operator: FilterOperator,
    value: String,
}

impl FilterCondition {
    pub fn new(table: &str, definition: &str) -> FilterCondition {
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
        let mut parts = self.value.split(".");
        let (Some(table), Some(field), None) = (parts.next(), parts.next(), parts.next()) else { panic!("malformatted reference field") };
        (table.to_string(), field.to_string())
    }
}

#[derive(Debug)]
#[derive(Default)]
#[derive(Clone)]
struct FieldFilters {
    table: String,
    field: String,
    position: Option<usize>,
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
            position: None,
            conditions,
        }
    }
}

impl FieldFilters {
    fn get_references(&self) -> Vec<(String, String)> {
        self.conditions.iter().filter(|x| x.is_reference()).cloned().map(|x| x.get_reference_parts()).collect()
    }

    fn test_value(&self, value: &str, references: &Option<&HashMap<String, HashSet<String>>>) -> bool {
        let direct = self.conditions.iter().filter(|x| !x.is_reference()).all(|condition| condition.test(value));
        if !direct {
            return false;
        }
        let Some(refs) = references else { return true };
        self.conditions.iter().filter(|x| x.is_reference()).all(|condition| {
            let Some(set) = refs.get(condition.value.as_str()) else { return false };
            set.contains(value)
        })
    }

    fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }
}

#[derive(Debug)]
#[derive(Default)]
#[derive(Clone)]
pub struct TableFilters {
    inner: HashMap<String, FieldFilters>,
}

impl TableFilters {
    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    fn has_resolved_positions(&self) -> bool {
        self.inner.values().all(|field_filters| {
            field_filters.position.is_some()
        })
    }

    fn resolve_positions(&mut self, insert_statement: &str) {
        let positions: HashMap<String, usize> = parse_insert_fields(insert_statement);
        for filter in self.inner.values_mut() {
            filter.set_position(positions[&filter.field])
        }
        assert!(self.has_resolved_positions());
    }

    pub fn test_values(
        &mut self,
        insert_statement: &str,
        references: &Option<&HashMap<String, HashSet<String>>>,
    ) -> bool {
        if self.is_empty() {
            return true;
        }
        if !self.has_resolved_positions() {
            self.resolve_positions(insert_statement);
        }

        let values = parse_insert_values(insert_statement);

        self.inner.values().all(|field_filters| {
            field_filters.position.is_some_and(|p| field_filters.test_value(values[p], references))
        })
    }

    pub fn get_references(&self) -> Vec<(String, String)> {
        self.inner.values().flat_map(|v| v.get_references()).collect()
    }
}

impl FromIterator<FilterCondition> for TableFilters {
    fn from_iter<T: IntoIterator<Item = FilterCondition>>(iter: T) -> Self {
        let conditions: Vec<FilterCondition> = iter.into_iter().collect();

        let distinct: Vec<&FilterCondition> = conditions.iter().unique_by(|s| &s.table).collect();
        if distinct.len() != 1 {
            panic!("conditions have different tables");
        }
        TableFilters {
            inner: conditions.into_iter().chunk_by(|x| x.field.clone()).into_iter().map(|(field, items)| (field, FieldFilters::from_iter(items))).collect(),
        }
    }
}


#[derive(Debug)]
#[derive(Default)]
pub struct Filters(HashMap<String, TableFilters>);

impl Filters {
    pub fn get_references_of_table(&self, table: &str) -> HashSet<String> {
        self.0.values().flat_map(|v| v.get_references()).filter(|(t, _)| t == table).map(|(_, f)| f).unique().collect()
    }

    pub fn get_filters_of_table(&self, key: &str) -> Option<TableFilters> {
        self.0.get(key).cloned()
    }
}

impl FromIterator<FilterCondition> for Filters {
    fn from_iter<T: IntoIterator<Item=FilterCondition>>(iter: T) -> Self {
        Filters (
            iter.into_iter().chunk_by(|x| x.table.clone()).into_iter().map(|(table, items)| (table, TableFilters::from_iter(items))).collect(),
        )
    }
}

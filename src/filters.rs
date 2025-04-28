use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::{parse_filter, parse_insert_fields, parse_insert_values};
use crate::references::References;

#[derive(Debug)]
enum FilterOperator {
    Equals,
    NotEquals,
    Reference,
    Unknown,
}

#[derive(Debug)]
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
                "->" => FilterOperator::Reference,
                _ => FilterOperator::Unknown,
            },
            value: value.to_string(),
        }
    }

    fn test(&self, other_value: &str) -> bool {
        match &self.operator {
            FilterOperator::Equals => self.value == other_value,
            FilterOperator::NotEquals => self.value != other_value,
            FilterOperator::Reference => true,
            FilterOperator::Unknown => true
        }
    }

    pub fn is_reference(&self) -> bool {
        match self.operator {
            FilterOperator::Reference => true,
            _ => false

        }
    }
}

#[derive(Debug)]
struct FieldFilters {
    table: String,
    field: String,
    position: Option<usize>,
    conditions: Vec<FilterCondition>,
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
    fn test_value(&self, value: &str, captured_references: &Option<&HashMap<String, HashSet<String>>>) -> bool {
        let direct = self.conditions.iter().filter(|x| !x.is_reference()).all(|condition| condition.test(value));
        if !direct {
            return false;
        }
        let Some(refs) = captured_references else { return true };
        self.conditions.iter().filter(|x| x.is_reference()).all(|condition| {
            let Some(set) = refs.get(condition.value.as_str()) else { return false };
            set.contains(value)
        })
    }

    fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }

    fn has_reference_filters(&self) -> bool {
        self.conditions.iter().any(|c| c.is_reference())
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct TableFilters {
    inner: HashMap<String, FieldFilters>,
}

impl TableFilters {
    pub fn has_filters(&self) -> bool {
        !self.inner.is_empty()
    }

    fn has_resolved_positions(&self) -> bool {
        self.inner.values().all(|field_filters| {
            field_filters.position.is_some()
        })
    }

    fn has_reference_filters(&self) -> bool {
        self.inner.values().any(|ff| ff.has_reference_filters())
    }

    fn resolve_positions(&mut self, insert_statement: &str) {
        let positions: HashMap<String, usize> = parse_insert_fields(insert_statement);
        for filter in self.inner.values_mut() {
            filter.set_position(positions[&filter.field])
        }
        assert!(self.has_resolved_positions());
    }

    pub fn test_sql_statement(
        &mut self,
        sql_statement: &str,
        captured_references: &Option<&HashMap<String, HashSet<String>>>,
    ) -> bool {
        if !sql_statement.starts_with("INSERT") {
            return false;
        }
        if self.has_filters() {
            if !self.has_resolved_positions() {
                self.resolve_positions(sql_statement);
            }
            let values = parse_insert_values(sql_statement);

            if !self.inner.values().all(|field_filters| {
                field_filters.position.is_some_and(|p| field_filters.test_value(values[p], captured_references))
            }) {
                return false;
            }
        }

        true
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
pub struct Filters{
    inner: HashMap<String, TableFilters>,
    pub references: References,
}

impl Filters {
    pub fn test_sql_statement(
        &mut self,
        sql_statement: &str,
        table: &Option<String>,
        captured_references: &Option<&HashMap<String, HashSet<String>>>,
    ) -> bool {
        let Some(t) = table else { return true };
        let should_keep = self.inner.get_mut(t).unwrap().test_sql_statement(sql_statement, captured_references);
        if should_keep {
            self.references.capture(t, sql_statement);
        }
        should_keep
    }

    pub fn get_tables_with_references(&self) -> HashSet<String> {
        self.inner.iter().filter(|(_, tf)| tf.has_reference_filters()).map(|(table, _)| table.clone()).collect()
    }
}

impl<'a> FromIterator<&'a (String, String)> for Filters {
    fn from_iter<T: IntoIterator<Item=&'a (String, String)>>(items: T) -> Self {
        let conditions: Vec<FilterCondition> = items.into_iter().map(|(table, condition)| FilterCondition::new(table, condition)).collect();
        let references = References::from_iter(conditions.iter().filter(|fc| fc.is_reference()).map(|fc| (fc.table.clone(), fc.field.clone())));

        let mut filters = Filters {
            inner: conditions.into_iter().chunk_by(|x| x.table.clone()).into_iter().map(|(table, items)| (table.clone(), {
                TableFilters::from_iter(items)
            })).collect(),
            references,
        };
        for table in filters.references.get_tables() {
            if !filters.inner.contains_key(table) {
                let tf = TableFilters::default();
                filters.inner.insert(table.to_string(), tf);
            }
        }
        filters
    }
}

pub fn filter_sql_lines<'a, I: Iterator<Item=String>>(
    filters: &'a mut Filters,
    references: Option<&'a HashMap<String, HashSet<String>>>,
    table: Option<String>,
    lines: I,
) -> impl Iterator<Item=String> {
    lines.filter(move |st| filters.test_sql_statement(st, &table, &references))
}

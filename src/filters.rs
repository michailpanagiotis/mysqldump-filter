use config::FileSource;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::{parse_filter, parse_insert_fields, parse_insert_values};
use crate::references::References;

fn group_conditions_by_table<'a>(conditions: &'a Vec<&'a FilterCondition>) -> HashMap<String, Vec<&'a FilterCondition>>  {
    let mut res: HashMap<String, Vec<&'a FilterCondition>>  = HashMap::new();
    for cond in conditions.iter() {
        if res.contains_key(&cond.table) {
            if let Some(val) = res.get_mut(&cond.table) { val.push(cond); };
        } else {
            res.insert(cond.table.clone(), Vec::new());
            if let Some(val) = res.get_mut(&cond.table) { val.push(cond); };
        }
    }
    res
}

fn group_conditions_by_field<'a>(table: &str, conditions: &'a Vec<&'a FilterCondition>) -> HashMap<String, Vec<&'a FilterCondition>>  {
    let mut res: HashMap<String, Vec<&'a FilterCondition>>  = HashMap::new();
    for cond in conditions.iter().filter(|c| c.table == table) {
        if res.contains_key(&cond.field) {
            if let Some(val) = res.get_mut(&cond.field) { val.push(cond); };
        } else {
            res.insert(cond.field.clone(), Vec::new());
            if let Some(val) = res.get_mut(&cond.field) { val.push(cond); };
        }
    }
    res
}

#[derive(Debug)]
#[derive(Clone)]
enum FilterOperator {
    Equals,
    NotEquals,
    ForeignKey,
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
                "->" => FilterOperator::ForeignKey,
                _ => FilterOperator::Unknown,
            },
            value: value.to_string(),
        }
    }

    fn test(&self, other_value: &str) -> bool {
        match &self.operator {
            FilterOperator::Equals => self.value == other_value,
            FilterOperator::NotEquals => self.value != other_value,
            FilterOperator::ForeignKey => true,
            FilterOperator::Unknown => true
        }
    }

    pub fn is_foreign_filter(&self) -> bool {
        matches!(self.operator, FilterOperator::ForeignKey)
    }

    pub fn get_foreign_key(&self) -> (String, String) {
        let mut split = self.value.split('.');
        let (Some(table), Some(field), None) = (split.next(), split.next(), split.next()) else {
            panic!("malformed foreign key {}", self.value);
        };
        (table.to_string(), field.to_string())
    }
}

#[derive(Debug)]
struct FieldFilters {
    table: String,
    field: String,
    position: Option<usize>,
    conditions: Vec<FilterCondition>,
}

impl FieldFilters {
    fn new<T: IntoIterator<Item = FilterCondition>>(iter: T) -> Self {
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

    fn test_value(&self, value: &str, foreign_values: &Option<&HashMap<String, HashSet<String>>>) -> bool {
        let direct = self.conditions.iter().filter(|x| !x.is_foreign_filter()).all(|condition| condition.test(value));
        if !direct {
            return false;
        }
        let Some(fvs) = foreign_values else { return true };
        self.conditions.iter().filter(|x| x.is_foreign_filter()).all(|condition| {
            let Some(set) = fvs.get(condition.value.as_str()) else { return false };
            set.contains(value)
        })
    }

    fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }

    fn has_foreign_filters(&self) -> bool {
        self.conditions.iter().any(|c| c.is_foreign_filter())
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct TableFilters<'a> {
    table: String,
    inner: HashMap<String, FieldFilters>,
    conditions: Vec<FilterCondition>,
    filter_conditions: HashMap<String, Vec<&'a FilterCondition>>,
}

impl<'a> TableFilters<'a> {
    fn new(table: &str, conditions: Vec<FilterCondition>, filter_conditions: &'a Vec<&'a FilterCondition>) -> Self {
        let conds: Vec<FilterCondition> = conditions.iter().cloned().collect();
        let distinct: Vec<&FilterCondition> = conditions.iter().unique_by(|s| &s.table).collect();
        if distinct.len() != 1 {
            panic!("conditions have different tables");
        }
        TableFilters {
            table: table.to_string(),
            inner: conditions.into_iter().chunk_by(|x| x.field.clone()).into_iter().map(|(field, items)| (field, FieldFilters::new(items))).collect(),
            conditions: conds,
            filter_conditions: group_conditions_by_field(table, filter_conditions),
        }
    }

    pub fn has_filters(&self) -> bool {
        !self.inner.is_empty()
    }

    fn has_resolved_positions(&self) -> bool {
        self.inner.values().all(|field_filters| {
            field_filters.position.is_some()
        })
    }

    fn has_foreign_filters(&self) -> bool {
        self.inner.values().any(|ff| ff.has_foreign_filters())
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
        foreign_values: &Option<&HashMap<String, HashSet<String>>>,
    ) -> bool {
        if !sql_statement.starts_with("INSERT") {
            return true;
        }
        if self.has_filters() {
            if !self.has_resolved_positions() {
                self.resolve_positions(sql_statement);
            }
            let values = parse_insert_values(sql_statement);

            if !self.inner.values().all(|field_filters| {
                field_filters.position.is_some_and(|p| field_filters.test_value(values[p], foreign_values))
            }) {
                return false;
            }
        }

        true
    }
}

// impl FromIterator<FilterCondition> for TableFilters {
//     fn from_iter<T: IntoIterator<Item = FilterCondition>>(iter: T) -> Self {
//         let conditions: Vec<FilterCondition> = iter.into_iter().collect();
//         TableFilters::new(conditions)
//     }
// }

#[derive(Debug)]
pub struct Filters<'a> {
    inner: HashMap<String, TableFilters<'a>>,
    filter_conditions: HashMap<String, Vec<&'a FilterCondition>>,
}

impl<'a> Filters<'a> {
    pub fn new(items: &'a Vec<(String, String)>, filter_conditions: &'a Vec<&'a FilterCondition>) -> Self {
        let conditions: Vec<FilterCondition> = items.iter().map(|(table, condition)| FilterCondition::new(table, condition)).collect();
        // let filter_conditions = group_conditions_by_table(filter_conditions);

        Filters {
            inner: conditions.into_iter().chunk_by(|x| x.table.clone()).into_iter().map(|(table, items)| (table.clone(), {
                TableFilters::new(&table, items.collect(), &filter_conditions)
            })).collect(),
            filter_conditions: group_conditions_by_table(filter_conditions),
        }
    }

    pub fn test_sql_statement(
        &mut self,
        sql_statement: &str,
        table: &Option<String>,
        foreign_values: &Option<&HashMap<String, HashSet<String>>>,
    ) -> bool {
        let Some(t) = table else { return true };
        let Some(f) = self.inner.get_mut(t) else { return true };
        f.test_sql_statement(sql_statement, foreign_values)
    }

    pub fn get_foreign_tables(&self) -> HashSet<String> {
        self.inner.iter().filter(|(_, tf)| tf.has_foreign_filters()).map(|(table, _)| table.clone()).collect()
    }
}

pub fn filter_sql_lines<'a, I: Iterator<Item=String>>(
    filters: &'a mut Filters,
    references: &'a mut References,
    foreign_values: Option<&'a HashMap<String, HashSet<String>>>,
    table: Option<String>,
    lines: I,
) -> impl Iterator<Item=String> {
    lines.filter(move |st| {
        let should_keep = filters.test_sql_statement(st, &table, &foreign_values);
        if should_keep {
            if let Some(ref t) = table {
                references.capture(t, st);
            }
        }
        should_keep
    })
}

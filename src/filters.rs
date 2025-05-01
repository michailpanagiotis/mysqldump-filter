use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::{FilterCondition, parse_insert_fields, parse_insert_values};
use crate::references::References;

#[derive(Debug)]
struct FieldFilters<'a> {
    field: String,
    position: Option<usize>,
    filter_conditions: Vec<&'a FilterCondition>,
}

impl<'a> FieldFilters<'a> {
    fn new(filter_conditions: &[&'a FilterCondition]) -> Self {
        assert!(filter_conditions.iter().unique_by(|s| (&s.table, &s.field)).count() == 1);

        FieldFilters {
            field: filter_conditions[0].field.clone(),
            position: None,
            filter_conditions: filter_conditions.to_vec(),
        }
    }

    fn test_value(&self, value: &str, foreign_values: &Option<&HashMap<String, HashSet<String>>>) -> bool {
        let direct = self.filter_conditions.iter().filter(|x| !x.is_foreign_filter()).all(|condition| condition.test(value));
        if !direct {
            return false;
        }
        self.filter_conditions.iter().filter(|x| x.is_foreign_filter()).all(|condition| {
            condition.test_foreign(value, foreign_values)
        })
    }

    fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }
}

#[derive(Debug)]
pub struct TableFilters<'a> {
    inner: HashMap<String, FieldFilters<'a>>,
}

impl<'a> TableFilters<'a> {
    fn new<'b>(filter_conditions: &'b Vec<&'a FilterCondition>) -> Self {
        assert!(filter_conditions.iter().unique_by(|s| &s.table).count() == 1);

        let inner: HashMap<String, FieldFilters<'a>> = filter_conditions.iter().chunk_by(|c| &c.field).into_iter().map(|(field, conds)| {
            let v: Vec<&'a FilterCondition> = conds.into_iter().copied().collect();
            (field.clone(), FieldFilters::new(&v))
        }).collect();

        TableFilters {
            inner,
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

#[derive(Debug)]
pub struct Filters<'a> {
    inner: HashMap<String, TableFilters<'a>>,
}

impl<'a> Filters<'a> {
    pub fn new<'b>(filter_conditions: &'b Vec<&'a FilterCondition>) -> Self
        where 'a : 'b
    {
        let inner = filter_conditions.iter().chunk_by(|c| &c.table).into_iter().map(|(table, conds)| {
            let v: Vec<&'a FilterCondition> = conds.into_iter().copied().collect();
            (table.clone(), TableFilters::new(&v))
        }).collect();


        Filters {
            inner,
        }
    }

    pub fn test_sql_statement(
        &mut self,
        sql_statement: &str,
        table: &str,
        foreign_values: &Option<&HashMap<String, HashSet<String>>>,
    ) -> bool {
        let Some(f) = self.inner.get_mut(table) else { return true };
        f.test_sql_statement(sql_statement, foreign_values)
    }
}

pub fn filter_statements<'a, I: Iterator<Item=(Option<String>, String)>>(
    filters: &'a mut Filters,
    references: &'a mut References,
    foreign_values: Option<&'a HashMap<String, HashSet<String>>>,
    lines: I,
) -> impl Iterator<Item=(Option<String>, String)> {
    lines.filter(move |(t, st)| {
        let Some(table) = t else { return true };
        let should_keep = filters.test_sql_statement(st, table, &foreign_values);
        if should_keep {
            references.capture(table, st);
        }
        should_keep
    })
}

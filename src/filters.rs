use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::expression_parser::{parse_filter, parse_insert_fields, parse_insert_values};

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq)]
enum FilterOperator {
    Equals,
    NotEquals,
    Reference,
    Unknown,
}

#[derive(Debug)]
#[derive(Clone)]
#[derive(Hash)]
#[derive(Eq, PartialEq)]
pub struct TableField {
    pub table: String,
    pub field: String,
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

    fn is_reference(&self) -> bool {
        self.operator == FilterOperator::Reference
    }

    fn get_referenced_field(&self) -> TableField {
        let mut parts = self.value.split(".");
        let (Some(table), Some(field), None) = (parts.next(), parts.next(), parts.next()) else { panic!("malformatted reference field") };
        TableField {
            table: table.to_string(),
            field: field.to_string()
        }
    }
}

#[derive(Debug)]
#[derive(Default)]
#[derive(Clone)]
struct FieldReference {
    table: String,
    field: String,
    position: Option<usize>,
    values: HashSet<String>,
}

impl FieldReference {
    fn new(table_field: &TableField) -> Self {
        FieldReference {
            table: table_field.table.to_string(),
            field: table_field.field.to_string(),
            position: None,
            values: HashSet::new()
        }
    }

    fn set_position(&mut self, pos: usize) {
        self.position = Some(pos);
    }

    fn capture(&mut self, value: &str) {
        self.values.insert(value.to_string());
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
}

#[derive(Debug)]
#[derive(Default)]
#[derive(Clone)]
pub struct TableReferences {
    inner: HashMap<String, FieldReference>,
}

impl TableReferences {
    pub fn has_referenced_fields(&self) -> bool {
        !self.inner.is_empty()
    }

    fn get_table(&self) -> Option<String> {
        Some(self.inner.values().next()?.table.clone())
    }

    fn has_resolved_positions(&self) -> bool {
        self.inner.values().all(|field_refs| {
            field_refs.position.is_some()
        })
    }

    fn resolve_positions(&mut self, insert_statement: &str) {
        let positions: HashMap<String, usize> = parse_insert_fields(insert_statement);
        for rf in self.inner.values_mut() {
            rf.set_position(positions[&rf.field])
        }
        assert!(self.has_resolved_positions());
    }

    pub fn capture(&mut self, insert_statement: &str) {
        if !self.has_referenced_fields() {
            return;
        }
        if !self.has_resolved_positions() {
            self.resolve_positions(insert_statement);
        }

        let values = parse_insert_values(insert_statement);

        self.inner.values_mut().for_each(|field_references| {
            let Some(pos) = field_references.position else { return };
            field_references.capture(values[pos]);
        })
    }
}

impl FromIterator<TableField> for TableReferences {
    fn from_iter<T: IntoIterator<Item = TableField>>(iter: T) -> Self {
        let fields: Vec<TableField> = iter.into_iter().collect();

        let distinct: Vec<&TableField> = fields.iter().unique_by(|s| &s.table).collect();
        if distinct.len() != 1 {
            panic!("fields have different tables");
        }
        TableReferences {
            inner: fields.into_iter().map(|table_field| (table_field.field.clone(), FieldReference::new(&table_field))).collect(),
        }
    }
}

impl From<&TableReferences> for HashMap<String, HashSet<String>> {
    fn from(item: &TableReferences) -> Self {
         item.inner.values().map(|field_reference| (field_reference.table.to_owned() + "." + field_reference.field.as_str(), field_reference.values.clone())).collect()
    }
}

#[derive(Debug)]
#[derive(Default)]
#[derive(Clone)]
pub struct TableFilters {
    inner: HashMap<String, FieldFilters>,
    pub references: TableReferences,
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

    fn resolve_positions(&mut self, insert_statement: &str) {
        let positions: HashMap<String, usize> = parse_insert_fields(insert_statement);
        for filter in self.inner.values_mut() {
            filter.set_position(positions[&filter.field])
        }
        assert!(self.has_resolved_positions());
    }

    pub fn test_insert_statement(
        &mut self,
        insert_statement: &str,
        captured_references: &Option<&HashMap<String, HashSet<String>>>,
    ) -> bool {
        if !insert_statement.starts_with("INSERT") {
            return false;
        }
        if self.has_filters() {
            if !self.has_resolved_positions() {
                self.resolve_positions(insert_statement);
            }
            let values = parse_insert_values(insert_statement);

            if !self.inner.values().all(|field_filters| {
                field_filters.position.is_some_and(|p| field_filters.test_value(values[p], captured_references))
            }) {
                return false;
            }
        }

        self.references.capture(insert_statement);

        true
    }

    fn set_references(&mut self, references: TableReferences) {
        self.references = references;
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
            references: TableReferences::default(),
        }
    }
}


#[derive(Debug)]
#[derive(Clone)]
#[derive(Default)]
pub struct References {
    pub inner: HashMap<String, TableReferences>
}

impl References {
    fn get_references_of_table(&self, key: &str) -> TableReferences {
        self.inner.get(key).cloned().unwrap_or_default()
    }
}

impl FromIterator<TableReferences> for References {
    fn from_iter<T: IntoIterator<Item=TableReferences>>(items: T) -> Self {
        let mut grouped: HashMap<String, TableReferences> = HashMap::new();
        for item in items.into_iter() {
            let Some(table) = item.get_table() else { continue };
            grouped.insert(table, item);
        }
        References {
            inner: grouped,
        }
    }
}

impl FromIterator<TableField> for References {
    fn from_iter<T: IntoIterator<Item=TableField>>(items: T) -> Self {
        let grouped: HashMap<String, Vec<TableField>> = items.into_iter().into_group_map_by(|f| f.table.clone());
        let inner: HashMap<String, TableReferences> = grouped.into_iter().map(|(table, tfs)| (table.to_string(), TableReferences::from_iter(tfs))).collect();
        References { inner }
    }
}

impl<'a> FromIterator<&'a FilterCondition> for References {
    fn from_iter<T: IntoIterator<Item=&'a FilterCondition>>(items: T) -> Self {
        let grouped = items.into_iter().filter(|fc| fc.is_reference()).map(|fc| fc.get_referenced_field()).into_group_map_by(|f| f.table.clone());
        let inner: HashMap<String, TableReferences> = grouped.into_iter().map(|(table, tfs)| (table.to_string(), TableReferences::from_iter(tfs))).collect();
        References { inner }
    }
}

impl From<References> for HashMap<String, HashSet<String>> {
    fn from(item: References) -> Self {
        let references: HashMap<String, HashSet<String>> = item.inner.values().fold(HashMap::new(), |mut acc, table_refs| {
            let rfs: HashMap<String, HashSet<String>> = HashMap::from(table_refs);
            acc.extend(rfs);
            acc
        });
        references
    }
}

#[derive(Debug)]
#[derive(Default)]
pub struct Filters{
    inner: HashMap<String, TableFilters>,
    references: References,
}

impl Filters {
    pub fn get_filters_of_table(&self, key: &str) -> Option<TableFilters> {
        self.inner.get(key).cloned()
    }
}

impl<'a> FromIterator<&'a (String, String)> for Filters {
    fn from_iter<T: IntoIterator<Item=&'a (String, String)>>(items: T) -> Self {
        let conditions: Vec<FilterCondition> = items.into_iter().map(|(table, condition)| FilterCondition::new(&table, &condition)).collect();
        let references = References::from_iter(conditions.iter());
        let mut filters = Filters {
            inner: conditions.into_iter().chunk_by(|x| x.table.clone()).into_iter().map(|(table, items)| (table.clone(), {
                let mut tf = TableFilters::from_iter(items);
                tf.set_references(references.get_references_of_table(&table));
                tf
            })).collect(),
            references,
        };
        for (table, table_references) in filters.references.inner.iter() {
            if !filters.inner.contains_key(table) {
                let mut tf = TableFilters::default();
                tf.set_references(table_references.clone());
                filters.inner.insert(table.to_string(), tf);
            }
        }
        filters
    }
}

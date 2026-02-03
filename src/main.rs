use clap::Parser;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tempdir::TempDir;

mod checks;
mod scanner;

use checks::get_passes;
use scanner::{count_records, gather, process_table_inserts, get_schema_tables};

/// Configuration structure for the mysqldump filter
///
/// This struct defines the filtering, cascading, and transformation rules
/// that will be applied to the MySQL dump file during processing.
#[derive(Debug)]
#[derive(Deserialize)]
#[serde(rename = "name")]
pub struct Config {
    /// Optional whitelist of tables whose data should be included in the output.
    /// If None, all tables are processed. If Some, only listed tables have their INSERT statements included.
    allow_data_on_tables: Option<HashSet<String>>,

    /// Defines cascading relationships between tables.
    /// Key: source table name
    /// Value: vector of cascade definitions in format "source_column->target_table.target_column"
    cascades: HashMap<String, Vec<String>>,

    /// Text replacement rules for anonymizing or sanitizing data.
    /// Outer key: table name
    /// Inner key: column name
    /// Value: replacement text
    text_transforms: HashMap<String, HashMap<String, String>>,

    /// Filter expressions using CEL (Common Expression Language).
    /// Key: table name
    /// Value: vector of CEL expressions that must evaluate to true for record inclusion
    filters: HashMap<String, Vec<String>>
}

impl Config {
    /// Load configuration from a JSON file
    ///
    /// # Arguments
    /// * `config_file` - Path to the JSON configuration file
    ///
    /// # Panics
    /// * If the config file path contains invalid UTF-8
    /// * If the config file cannot be read
    /// * If the JSON is malformed or doesn't match the expected structure
    fn from_file(config_file: &Path) -> Self {
        let file = config::File::new(config_file.to_str().expect("invalid config path"), config::FileFormat::Json);
        let settings = config::Config::builder().add_source(file).build().expect("cannot read config file");
        settings.try_deserialize::<Config>().expect("malformed config")
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
enum Cli {
    /// Filter a mysqldump file according to configuration
    Filter(FilterCli),
    /// Count records per table in a mysqldump file
    Count(CountCli),
}

#[derive(Parser, Debug)]
struct FilterCli {
    /// Input MySQL dump file to process
    #[clap(value_name = "FILE", required=true)]
    input: PathBuf,

    /// Path to JSON configuration file
    #[clap(short, long, required = true)]
    config: PathBuf,

    /// Output file path for filtered dump
    #[clap(short, long, required = true)]
    output: PathBuf,

    /// Optional working directory for temporary files (defaults to system temp)
    #[clap(short, long, required = false)]
    working_dir: Option<PathBuf>,

    /// Show the execution plan only, without running the filter
    #[clap(long)]
    plan_only: bool,

    /// Run only the first N passes (1-indexed, defaults to all)
    #[clap(long)]
    passes: Option<usize>,
}

#[derive(Parser, Debug)]
struct CountCli {
    /// Input MySQL dump file to count records in
    #[clap(value_name = "FILE")]
    input: PathBuf,
}

fn main() -> Result<(), anyhow::Error> {
    match Cli::parse() {
        Cli::Filter(args) => run_filter(args),
        Cli::Count(args) => run_count(args),
    }
}

fn run_filter(cli: FilterCli) -> Result<(), anyhow::Error> {
    let input_file = std::env::current_dir()?.join(&cli.input);
    let output_file = std::env::current_dir()?.join(&cli.output);
    let config_file = std::env::current_dir()?.join(&cli.config);
    let temp_dir = if cli.working_dir.is_none() { Some(TempDir::new("sql_parser").expect("cannot create temporary dir")) } else { None };
    let config = Config::from_file(config_file.as_path());

    let working_dir_path = match temp_dir {
        Some(ref dir) => dir.path().to_path_buf(),
        None => cli.working_dir.unwrap(),
    };
    let working_file_path = working_dir_path.join("INTERIM").with_extension("sql");

    let schema_tables = get_schema_tables(&input_file)?;

    let passes = get_passes(
        config.cascades.iter().chain(&config.filters),
        config.text_transforms,
        config.allow_data_on_tables,
        schema_tables,
    )?;
    passes.print_plan();

    let run_passes = match cli.passes {
        Some(n) if n >= 1 && n <= passes.len() => n,
        Some(n) => return Err(anyhow::anyhow!("--passes must be between 1 and {}, got {n}", passes.len())),
        None => passes.len(),
    };

    if cli.plan_only {
        return Ok(());
    }

    let mut lookup_table = HashMap::new();
    for pending_tables in passes.into_iter().take(run_passes) {
        for (table, table_checks) in pending_tables {
            process_table_inserts(
                &working_file_path,
                &table,
                |statement| {
                    table_checks.apply(statement, &mut lookup_table)
                },
            )?;
        }
    }

    gather(&working_file_path, &output_file)?;

    if let Some(dir) = temp_dir {
       let _ = dir.close();
    }

    Ok(())
}

fn run_count(cli: CountCli) -> Result<(), anyhow::Error> {
    let input_file = std::env::current_dir()?.join(&cli.input);
    let counts = count_records(&input_file)?;

    if counts.is_empty() {
        println!("No tables found.");
        return Ok(());
    }

    let max_name_len = counts.iter().map(|(name, _)| name.len()).max().unwrap_or(0).max("Total".len());
    let total: usize = counts.iter().map(|(_, count)| count).sum();
    let max_count_len = format!("{total}").len();

    for (table, count) in &counts {
        println!("{:<width$}  {:>cwidth$}", table, count, width = max_name_len, cwidth = max_count_len);
    }
    println!("{:-<width$}", "", width = max_name_len + 2 + max_count_len);
    println!("{:<width$}  {:>cwidth$}", "Total", total, width = max_name_len, cwidth = max_count_len);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use std::fs::File;
    use std::io::Write;

    fn create_test_config() -> Config {
        Config {
            allow_data_on_tables: Some(HashSet::from([
                "users".to_string(),
                "profiles".to_string()
            ])),
            cascades: HashMap::from([
                ("users".to_string(), vec!["id->profiles.user_id".to_string()])
            ]),
            text_transforms: HashMap::from([
                ("users".to_string(), HashMap::from([
                    ("email".to_string(), "anonymized@example.com".to_string())
                ]))
            ]),
            filters: HashMap::from([
                ("users".to_string(), vec!["active == true".to_string()])
            ])
        }
    }

    fn create_test_config_file(content: &str) -> Result<TempDir, anyhow::Error> {
        let temp_dir = TempDir::new("config_test")?;
        let config_path = temp_dir.path().join("config.json");
        let mut file = File::create(&config_path)?;
        file.write_all(content.as_bytes())?;
        Ok(temp_dir)
    }

    #[test]
    fn test_config_from_file_valid() {
        let config_content = r#"{
            "allow_data_on_tables": ["users", "profiles"],
            "cascades": {
                "users": ["id->profiles.user_id"]
            },
            "text_transforms": {
                "users": {
                    "email": "anonymized@example.com"
                }
            },
            "filters": {
                "users": ["active == true"]
            }
        }"#;

        let temp_dir = create_test_config_file(config_content).unwrap();
        let config_path = temp_dir.path().join("config.json");

        let config = Config::from_file(&config_path);

        assert!(config.allow_data_on_tables.is_some());
        let allowed_tables = config.allow_data_on_tables.unwrap();
        assert!(allowed_tables.contains("users"));
        assert!(allowed_tables.contains("profiles"));

        assert!(config.cascades.contains_key("users"));
        assert_eq!(config.cascades["users"], vec!["id->profiles.user_id"]);

        assert!(config.text_transforms.contains_key("users"));
        assert_eq!(config.text_transforms["users"]["email"], "anonymized@example.com");

        assert!(config.filters.contains_key("users"));
        assert_eq!(config.filters["users"], vec!["active == true"]);
    }

    #[test]
    fn test_config_from_file_minimal() {
        let config_content = r#"{
            "cascades": {},
            "text_transforms": {},
            "filters": {}
        }"#;

        let temp_dir = create_test_config_file(config_content).unwrap();
        let config_path = temp_dir.path().join("config.json");

        let config = Config::from_file(&config_path);

        assert!(config.allow_data_on_tables.is_none());
        assert!(config.cascades.is_empty());
        assert!(config.text_transforms.is_empty());
        assert!(config.filters.is_empty());
    }

    #[test]
    #[should_panic(expected = "cannot read config file")]
    fn test_config_from_file_invalid_json() {
        let config_content = r#"{ invalid json }"#;

        let temp_dir = create_test_config_file(config_content).unwrap();
        let config_path = temp_dir.path().join("config.json");

        Config::from_file(&config_path);
    }

    #[test]
    #[should_panic(expected = "cannot read config file")]
    fn test_config_from_file_nonexistent() {
        let temp_dir = TempDir::new("config_test").unwrap();
        let nonexistent_path = temp_dir.path().join("nonexistent.json");

        Config::from_file(&nonexistent_path);
    }

    #[test]
    fn test_config_structure_properties() {
        let config = create_test_config();

        // Test allow_data_on_tables
        assert!(config.allow_data_on_tables.is_some());
        let allowed = config.allow_data_on_tables.unwrap();
        assert_eq!(allowed.len(), 2);
        assert!(allowed.contains("users"));
        assert!(allowed.contains("profiles"));

        // Test cascades
        assert_eq!(config.cascades.len(), 1);
        assert!(config.cascades.contains_key("users"));
        assert_eq!(config.cascades["users"].len(), 1);
        assert_eq!(config.cascades["users"][0], "id->profiles.user_id");

        // Test text_transforms
        assert_eq!(config.text_transforms.len(), 1);
        assert!(config.text_transforms.contains_key("users"));
        assert_eq!(config.text_transforms["users"].len(), 1);
        assert_eq!(config.text_transforms["users"]["email"], "anonymized@example.com");

        // Test filters
        assert_eq!(config.filters.len(), 1);
        assert!(config.filters.contains_key("users"));
        assert_eq!(config.filters["users"].len(), 1);
        assert_eq!(config.filters["users"][0], "active == true");
    }

    #[test]
    fn test_config_with_multiple_filters_and_cascades() {
        let config_content = r#"{
            "cascades": {
                "users": ["id->profiles.user_id", "id->orders.customer_id"],
                "orders": ["product_id->products.id"]
            },
            "text_transforms": {
                "users": {
                    "email": "user@example.com",
                    "phone": "555-0000"
                },
                "profiles": {
                    "address": "123 Main St"
                }
            },
            "filters": {
                "users": ["active == true", "created_at > timestamp('2023-01-01')"],
                "orders": ["status == 'completed'"]
            }
        }"#;

        let temp_dir = create_test_config_file(config_content).unwrap();
        let config_path = temp_dir.path().join("config.json");

        let config = Config::from_file(&config_path);

        // Test multiple cascades
        assert_eq!(config.cascades["users"].len(), 2);
        assert!(config.cascades["users"].contains(&"id->profiles.user_id".to_string()));
        assert!(config.cascades["users"].contains(&"id->orders.customer_id".to_string()));
        assert_eq!(config.cascades["orders"], vec!["product_id->products.id"]);

        // Test multiple text transforms
        assert_eq!(config.text_transforms["users"].len(), 2);
        assert_eq!(config.text_transforms["users"]["email"], "user@example.com");
        assert_eq!(config.text_transforms["users"]["phone"], "555-0000");
        assert_eq!(config.text_transforms["profiles"]["address"], "123 Main St");

        // Test multiple filters
        assert_eq!(config.filters["users"].len(), 2);
        assert!(config.filters["users"].contains(&"active == true".to_string()));
        assert!(config.filters["users"].contains(&"created_at > timestamp('2023-01-01')".to_string()));
        assert_eq!(config.filters["orders"], vec!["status == 'completed'"]);
    }
}

# mysqldump-filter

A powerful Rust tool for filtering and transforming MySQL dump files based on configurable rules and dependencies.

## Overview

`mysqldump-filter` processes MySQL dump files by applying customizable filters, cascading rules, and text transformations to selectively include or exclude data based on complex criteria. It's particularly useful for creating sanitized database dumps, extracting specific data subsets, or transforming sensitive data.

## Features

- **Configurable Filtering**: Define custom filters using CEL (Common Expression Language) expressions
- **Cascading Dependencies**: Automatically include related records based on foreign key relationships
- **Text Transformations**: Replace sensitive data with sanitized values
- **SQL Parsing**: Robust parsing of MySQL dump files with support for INSERT statements and table structures
- **Efficient Processing**: Streams large dump files without loading everything into memory

## Installation

Build from source using Cargo:

```bash
git clone <repository-url>
cd mysqldump-filter
cargo build --release
```

The binary will be available at `target/release/filter`.

## Usage

```bash
filter <INPUT_FILE> --config <CONFIG_FILE> --output <OUTPUT_FILE> [--working-dir <WORKING_DIR>]
```

### Arguments

- `INPUT_FILE`: Path to the MySQL dump file to process
- `--config`: Path to the JSON configuration file
- `--output`: Path where the filtered output will be written
- `--working-dir` (optional): Directory for temporary files (defaults to system temp)

### Example

```bash
./filter dump.sql --config config.json --output filtered_dump.sql
```

## Configuration

The tool uses a JSON configuration file with the following structure:

```json
{
  "allow_data_on_tables": ["table1", "table2"],
  "cascades": {
    "users": ["user_id->profiles.user_id", "user_id->orders.customer_id"]
  },
  "filters": {
    "users": ["active == true", "created_at > timestamp('2023-01-01')"]
  },
  "text_transforms": {
    "users": {
      "email": "anonymized@example.com",
      "phone": "000-000-0000"
    }
  }
}
```

### Configuration Options

#### `allow_data_on_tables` (optional)
Array of table names that should have their data included. If specified, only these tables will have INSERT statements processed.

#### `cascades`
Defines foreign key relationships for cascading inclusion. When a record matches filter criteria, related records in dependent tables are automatically included.

Format: `"source_column->target_table.target_column"`

#### `filters`
CEL expressions that determine which records to include. Each expression should evaluate to a boolean.

Supported operators:
- Comparison: `==`, `!=`, `<`, `<=`, `>`, `>=`
- Logical: `&&`, `||`, `!`
- Functions: `timestamp()` for date parsing

#### `text_transforms`
Replace specific column values with sanitized alternatives. Useful for anonymizing sensitive data.

## How It Works

1. **Parse Configuration**: Loads filtering rules, dependencies, and transformations
2. **Analyze Dependencies**: Builds a dependency graph to determine processing order
3. **Process in Passes**: Processes tables in multiple passes to handle cascading relationships
4. **Apply Filters**: Tests each record against configured CEL expressions
5. **Transform Data**: Applies text transformations to specified columns
6. **Generate Output**: Writes filtered and transformed SQL dump

## Architecture

The codebase is organized into several key modules:

### `src/main.rs`
- Command-line argument parsing
- Configuration loading
- Main processing orchestration

### `src/scanner/`
- **`mod.rs`**: Core SQL parsing and statement processing
- **`sql_parser.rs`**: SQL parsing utilities for extracting table metadata
- **`writers.rs`**: Output file management

### `src/checks/`
- **`mod.rs`**: Filter implementations (CEL, lookup, tracking)
- **`dependencies.rs`**: Dependency graph management

## Data Types and Structures

### Key Types

- `SqlStatement`: Represents a parsed SQL statement with table context
- `DBMeta`: Metadata about database schema (column types, positions)
- `TableChecks`: Collection of filters for a specific table
- `PlainColumnCheck`: Trait for different types of column validation

### Filter Types

1. **CEL Filters**: Evaluate complex expressions against column values
2. **Lookup Filters**: Check if values exist in dependency tables
3. **Tracking Filters**: Collect values for use by dependent filters

## Examples

### Basic Filtering
Filter active users created after a specific date:

```json
{
  "filters": {
    "users": ["active == true && created_at > timestamp('2023-01-01')"]
  }
}
```

### Cascading Relationships
Include user profiles when users are included:

```json
{
  "cascades": {
    "users": ["user_id->profiles.user_id"]
  },
  "filters": {
    "users": ["premium == true"]
  }
}
```

### Data Anonymization
Replace sensitive user data:

```json
{
  "text_transforms": {
    "users": {
      "email": "user@example.com",
      "phone": "555-0000",
      "ssn": "XXX-XX-XXXX"
    }
  }
}
```

## Performance Considerations

- The tool processes large dump files in streaming fashion
- Temporary files are used to handle multi-pass processing
- Memory usage scales with the number of tracked values, not file size
- Processing time increases with filter complexity and dependency depth

## Limitations

- Currently supports MySQL dump format only
- CEL expressions have limited function support
- Complex multi-table joins require manual cascade configuration
- Schema changes during processing are not supported

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Dependencies

- `clap`: Command-line argument parsing
- `serde`: JSON serialization/deserialization
- `sqlparser`: SQL parsing
- `cel-interpreter`: CEL expression evaluation
- `anyhow`: Error handling
- `regex`: Pattern matching
- `chrono`: Date/time handling

## License

[Add license information here]
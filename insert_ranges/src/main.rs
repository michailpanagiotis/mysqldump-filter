//! Discover INSERT statement byte ranges in MySQL dump files using interpolation search.
//!
//! This tool efficiently finds the byte ranges of INSERT statements in large MySQL dump files
//! without scanning the entire file linearly. It uses interpolation search to achieve O(log n)
//! complexity for finding table boundaries.

use anyhow::Result;
use clap::Parser;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

// ============================================================================
// CLI
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "insert_ranges")]
#[command(about = "Discover INSERT statement byte ranges in MySQL dump files")]
#[command(version)]
struct Cli {
    /// Input MySQL dump file
    #[clap(value_name = "FILE")]
    input: PathBuf,

    /// Don't group LOCK/INSERT/UNLOCK into single table ranges
    #[clap(long)]
    no_group: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let input_file = std::env::current_dir()?.join(&cli.input);
    let ranges = discover_ranges(&input_file)?;

    if ranges.is_empty() {
        println!("No ranges found.");
        return Ok(());
    }

    // Group Lock + Insert + Unlock into single table ranges (unless --no-group)
    let ranges = if cli.no_group {
        ranges
    } else {
        merge_table_data(ranges)
    };

    // Calculate column widths
    let max_kind_len = ranges
        .iter()
        .map(|r| format_kind(&r.kind).len())
        .max()
        .unwrap_or(0);

    let max_start_len = ranges.iter().map(|r| format!("{}", r.start).len()).max().unwrap_or(0);
    let max_size_len = ranges.iter().map(|r| format!("{}", r.end - r.start).len()).max().unwrap_or(0);
    let max_human_len = ranges
        .iter()
        .map(|r| humanize_bytes(r.end - r.start).len())
        .max()
        .unwrap_or(0);

    // Print header
    println!(
        "{:<kw$}  {:>sw$}  {:>zw$}  {:>hw$}",
        "Kind", "Start", "Size", "",
        kw = max_kind_len, sw = max_start_len, zw = max_size_len, hw = max_human_len
    );
    println!(
        "{:-<kw$}  {:-<sw$}  {:-<zw$}  {:-<hw$}",
        "", "", "", "",
        kw = max_kind_len, sw = max_start_len, zw = max_size_len, hw = max_human_len
    );

    // Print ranges
    for range in &ranges {
        let size = range.end - range.start;
        println!(
            "{:<kw$}  {:>sw$}  {:>zw$}  {:>hw$}",
            format_kind(&range.kind), range.start, size, humanize_bytes(size),
            kw = max_kind_len, sw = max_start_len, zw = max_size_len, hw = max_human_len
        );
    }

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

fn humanize_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes_f = bytes as f64;
    if bytes_f >= GIB {
        format!("{:.2} GiB", bytes_f / GIB)
    } else if bytes_f >= MIB {
        format!("{:.2} MiB", bytes_f / MIB)
    } else if bytes_f >= KIB {
        format!("{:.2} KiB", bytes_f / KIB)
    } else {
        format!("{} B", bytes)
    }
}

fn format_kind(kind: &RangeKind) -> String {
    match kind {
        RangeKind::Insert(table) => format!("INSERT({})", table),
        RangeKind::DumpingData(table) => format!("DUMPING({})", table),
        RangeKind::Lock(table) => format!("LOCK({})", table),
        RangeKind::Unlock => "UNLOCK".to_string(),
        RangeKind::DisableKeys(table) => format!("DISABLE_KEYS({})", table),
        RangeKind::EnableKeys(table) => format!("ENABLE_KEYS({})", table),
        RangeKind::Other => "Other".to_string(),
    }
}

// ============================================================================
// Range Discovery
// ============================================================================

/// Classification of a statement range
#[derive(Debug, Clone, PartialEq, Eq)]
enum RangeKind {
    /// INSERT statements for a specific table
    Insert(String),
    /// "-- Dumping data for table `table`" comment
    DumpingData(String),
    /// LOCK TABLES statement for a specific table
    Lock(String),
    /// UNLOCK TABLES statement
    Unlock,
    /// /*!40000 ALTER TABLE `table` DISABLE KEYS */
    DisableKeys(String),
    /// /*!40000 ALTER TABLE `table` ENABLE KEYS */
    EnableKeys(String),
    /// Everything else (schema, control statements, comments, etc.)
    Other,
}

/// A contiguous range of bytes with the same classification
#[derive(Debug)]
struct FileRange {
    /// Start byte offset (inclusive)
    start: u64,
    /// End byte offset (exclusive)
    end: u64,
    /// Classification of this range
    kind: RangeKind,
}

/// Information about a single statement
struct StatementInfo {
    kind: RangeKind,
    start: u64,
    end: u64,
}

/// Discover ranges in a MySQL dump file using interpolation search.
///
/// Uses interpolation search to find statement boundaries:
/// 1. Classify first and last statements
/// 2. Find gaps between known statements
/// 3. Sample midpoint of each gap to discover new statements
/// 4. Repeat until no gaps remain (all statements are adjacent)
///
/// When both endpoints and midpoint of a range have the same classification,
/// the entire range is considered homogeneous (no further sampling needed).
fn discover_ranges(filepath: &Path) -> Result<Vec<FileRange>> {
    let mut file = File::open(filepath)?;
    let file_size = file.metadata()?.len();

    if file_size == 0 {
        return Ok(vec![]);
    }

    // Known statements, keyed by start position
    let mut known: BTreeMap<u64, StatementInfo> = BTreeMap::new();

    // Classify first statement (treat position 0 as if preceded by ";\n")
    let first = classify_position(&mut file, 0, file_size)?;
    known.insert(first.start, first);

    // Classify last statement
    let last = classify_position(&mut file, file_size.saturating_sub(1), file_size)?;
    if !known.contains_key(&last.start) {
        known.insert(last.start, last);
    }

    // Iteratively fill gaps using interpolation
    loop {
        let gaps = find_gaps(&known, file_size);
        if gaps.is_empty() {
            break;
        }

        for gap in gaps {
            // Sample midpoint
            let mid = gap.start + (gap.end - gap.start) / 2;
            let stmt = classify_position(&mut file, mid, file_size)?;

            // If left, mid, and right are all INSERT for the same table, range is homogeneous.
            // We only do this for INSERT because Other statements are scattered throughout
            // the file (LOCK/UNLOCK around each table) and are not truly contiguous.
            if let (Some(lk), Some(rk)) = (&gap.left_kind, &gap.right_kind) {
                if let (RangeKind::Insert(lt), RangeKind::Insert(mt), RangeKind::Insert(rt)) =
                    (lk, &stmt.kind, rk)
                {
                    if lt == mt && mt == rt {
                        // All three are INSERT for the same table - range is homogeneous
                        known.insert(
                            gap.start,
                            StatementInfo {
                                kind: stmt.kind,
                                start: gap.start,
                                end: gap.end,
                            },
                        );
                        continue;
                    }
                }
            }

            // Different kinds found, or at file boundary - add the discovered statement
            known.insert(stmt.start, stmt);
        }
    }

    // Convert to FileRange and merge adjacent same-kind ranges
    let mut ranges: Vec<FileRange> = known
        .into_values()
        .map(|s| FileRange {
            start: s.start,
            end: s.end,
            kind: s.kind,
        })
        .collect();

    ranges.sort_by_key(|r| r.start);
    Ok(merge_ranges(ranges))
}

/// A gap between known statements, with references to adjacent statement kinds
struct Gap {
    start: u64,
    end: u64,
    left_kind: Option<RangeKind>,
    right_kind: Option<RangeKind>,
}

/// Find gaps between known statements, including the kinds of adjacent statements
fn find_gaps(known: &BTreeMap<u64, StatementInfo>, file_size: u64) -> Vec<Gap> {
    let mut gaps = Vec::new();
    let mut prev_end = 0u64;
    let mut prev_kind: Option<RangeKind> = None;

    let stmts: Vec<_> = known.values().collect();

    for stmt in stmts.iter() {
        if stmt.start > prev_end {
            // There's a gap between prev_end and stmt.start
            gaps.push(Gap {
                start: prev_end,
                end: stmt.start,
                left_kind: prev_kind.clone(),
                right_kind: Some(stmt.kind.clone()),
            });
        }
        prev_end = prev_end.max(stmt.end);
        prev_kind = Some(stmt.kind.clone());
    }

    // Check for gap at end of file
    if prev_end < file_size {
        gaps.push(Gap {
            start: prev_end,
            end: file_size,
            left_kind: prev_kind,
            right_kind: None,
        });
    }

    gaps
}

/// Classify the statement at a given byte position.
fn classify_position(file: &mut File, pos: u64, file_size: u64) -> Result<StatementInfo> {
    // Find statement boundaries
    let stmt_start = find_statement_start(file, pos)?;
    let stmt_end = find_statement_end(file, stmt_start, file_size)?;

    // Read the beginning of the statement to classify it
    file.seek(SeekFrom::Start(stmt_start))?;

    let bytes_to_read = std::cmp::min(512, (stmt_end - stmt_start) as usize);
    let mut buf = vec![0u8; bytes_to_read];
    let n = file.read(&mut buf)?;

    // Use lossy conversion since the buffer may cut in the middle of a multi-byte UTF-8 character.
    // We only need to parse ASCII keywords at the start ("INSERT INTO `table`"), so this is safe.
    let text = String::from_utf8_lossy(&buf[..n]);
    let trimmed = text.trim_start();

    let kind = if let Some(rest) = trimmed.strip_prefix("INSERT") {
        // Extract table name from "INSERT INTO `table_name`"
        if let Some(table) = extract_insert_table(rest) {
            RangeKind::Insert(table)
        } else {
            RangeKind::Other
        }
    } else if let Some(rest) = trimmed.strip_prefix("LOCK TABLES") {
        // Extract table name from "LOCK TABLES `table_name` WRITE"
        if let Some(table) = extract_lock_table(rest) {
            RangeKind::Lock(table)
        } else {
            RangeKind::Other
        }
    } else if trimmed.starts_with("UNLOCK TABLES") {
        RangeKind::Unlock
    } else if let Some(table) = extract_alter_keys(trimmed, "DISABLE") {
        RangeKind::DisableKeys(table)
    } else if let Some(table) = extract_alter_keys(trimmed, "ENABLE") {
        RangeKind::EnableKeys(table)
    } else if let Some(table) = extract_dumping_data_comment(trimmed) {
        RangeKind::DumpingData(table)
    } else {
        RangeKind::Other
    };

    Ok(StatementInfo {
        kind,
        start: stmt_start,
        end: stmt_end,
    })
}

/// Search backward from pos to find ";\n" delimiter or start of file.
/// Returns the byte position where the statement starts (after the delimiter).
fn find_statement_start(file: &mut File, pos: u64) -> Result<u64> {
    if pos == 0 {
        return Ok(0);
    }

    const CHUNK_SIZE: u64 = 8192;
    let mut search_end = pos;

    loop {
        let chunk_start = search_end.saturating_sub(CHUNK_SIZE);
        let chunk_len = (search_end - chunk_start) as usize;

        if chunk_len == 0 {
            return Ok(0);
        }

        file.seek(SeekFrom::Start(chunk_start))?;
        let mut buf = vec![0u8; chunk_len];
        file.read_exact(&mut buf)?;

        // Search backward for ";\n"
        if buf.len() >= 2 {
            for i in (0..buf.len() - 1).rev() {
                if buf[i] == b';' && buf[i + 1] == b'\n' {
                    // Statement starts after ";\n"
                    return Ok(chunk_start + i as u64 + 2);
                }
            }
        }

        if chunk_start == 0 {
            return Ok(0);
        }

        // Continue searching in previous chunk
        // Keep 1 byte overlap to catch ";\n" split across chunks
        search_end = chunk_start + 1;
    }
}

/// Search forward from pos to find ";\n" delimiter or end of file.
/// Returns the byte position after the delimiter (exclusive end).
fn find_statement_end(file: &mut File, pos: u64, file_size: u64) -> Result<u64> {
    const CHUNK_SIZE: usize = 8192;
    let mut search_pos = pos;

    file.seek(SeekFrom::Start(search_pos))?;

    loop {
        let mut buf = [0u8; CHUNK_SIZE];
        let n = file.read(&mut buf)?;

        if n == 0 {
            return Ok(file_size);
        }

        // Search forward for ";\n"
        for i in 0..n.saturating_sub(1) {
            if buf[i] == b';' && buf[i + 1] == b'\n' {
                // Statement ends after ";\n"
                return Ok(search_pos + i as u64 + 2);
            }
        }

        // Handle ";\n" split across chunks
        if n > 0 && buf[n - 1] == b';' {
            // Check if next byte is '\n'
            let mut next = [0u8; 1];
            if file.read(&mut next)? == 1 && next[0] == b'\n' {
                return Ok(search_pos + n as u64 + 1);
            }
        }

        search_pos += n as u64;

        if search_pos >= file_size {
            return Ok(file_size);
        }
    }
}

/// Extract table name from the part after "INSERT" keyword.
fn extract_insert_table(after_insert: &str) -> Option<String> {
    let text = after_insert.trim_start();
    let text = text.strip_prefix("INTO")?;
    let text = text.trim_start();
    extract_table_name(text)
}

/// Extract table name from the part after "LOCK TABLES" keyword.
fn extract_lock_table(after_lock: &str) -> Option<String> {
    let text = after_lock.trim_start();
    extract_table_name(text)
}

/// Extract a table name from text, handling backtick-quoted and unquoted names.
fn extract_table_name(text: &str) -> Option<String> {
    if text.starts_with('`') {
        // Backtick-quoted: `table_name`
        let end = text[1..].find('`')?;
        Some(text[1..=end].to_string())
    } else {
        // Unquoted: table_name
        let end = text.find(|c: char| c.is_whitespace() || c == '(')?;
        Some(text[..end].to_string())
    }
}

/// Extract table name from "/*!40000 ALTER TABLE `table` DISABLE/ENABLE KEYS */"
fn extract_alter_keys(text: &str, key_action: &str) -> Option<String> {
    if !text.starts_with("/*!") {
        return None;
    }

    if !text.contains("ALTER TABLE") {
        return None;
    }

    let keys_pattern = format!("{} KEYS", key_action);
    if !text.contains(&keys_pattern) {
        return None;
    }

    let alter_pos = text.find("ALTER TABLE")?;
    let after_alter = &text[alter_pos + 11..];
    let trimmed = after_alter.trim_start();
    extract_table_name(trimmed)
}

/// Extract table name from "-- Dumping data for table `table_name`" comment
fn extract_dumping_data_comment(text: &str) -> Option<String> {
    if !text.starts_with("--") {
        return None;
    }

    let pattern = "Dumping data for table";
    let pos = text.find(pattern)?;
    let after_pattern = &text[pos + pattern.len()..];
    let trimmed = after_pattern.trim_start();
    extract_table_name(trimmed)
}

/// Merge adjacent ranges with the same kind.
fn merge_ranges(ranges: Vec<FileRange>) -> Vec<FileRange> {
    if ranges.is_empty() {
        return ranges;
    }

    let mut merged = Vec::with_capacity(ranges.len());
    let mut iter = ranges.into_iter();
    let mut current = iter.next().unwrap();

    for next in iter {
        if current.end == next.start && current.kind == next.kind {
            current.end = next.end;
        } else {
            merged.push(current);
            current = next;
        }
    }
    merged.push(current);

    merged
}

/// Merge adjacent table data ranges for the same table.
/// Groups: DumpingData comment, LOCK TABLES, DISABLE KEYS, INSERT statements,
/// ENABLE KEYS, UNLOCK TABLES into a single INSERT range per table.
fn merge_table_data(ranges: Vec<FileRange>) -> Vec<FileRange> {
    if ranges.is_empty() {
        return ranges;
    }

    let mut merged = Vec::with_capacity(ranges.len());
    let mut iter = ranges.into_iter().peekable();

    while let Some(current) = iter.next() {
        // Check if this starts a table data sequence
        let (table, start, mut end) = match &current.kind {
            RangeKind::DumpingData(t) => (t.clone(), current.start, current.end),
            RangeKind::Lock(t) => (t.clone(), current.start, current.end),
            _ => {
                merged.push(current);
                continue;
            }
        };

        // Consume all following ranges that belong to this table's data
        while let Some(next) = iter.peek() {
            if next.start != end {
                break;
            }
            match &next.kind {
                RangeKind::DumpingData(t) if t == &table => {
                    end = next.end;
                    iter.next();
                }
                RangeKind::Lock(t) if t == &table => {
                    end = next.end;
                    iter.next();
                }
                RangeKind::Insert(t) if t == &table => {
                    end = next.end;
                    iter.next();
                }
                RangeKind::DisableKeys(t) if t == &table => {
                    end = next.end;
                    iter.next();
                }
                RangeKind::EnableKeys(t) if t == &table => {
                    end = next.end;
                    iter.next();
                }
                RangeKind::Unlock => {
                    end = next.end;
                    iter.next();
                    break;
                }
                _ => break,
            }
        }

        merged.push(FileRange {
            start,
            end,
            kind: RangeKind::Insert(table),
        });
    }

    merged
}

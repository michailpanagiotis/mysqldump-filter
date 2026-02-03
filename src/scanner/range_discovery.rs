//! Range discovery for MySQL dump files using interpolation search.
//!
//! This module provides efficient detection of INSERT statement ranges
//! without linear scanning the entire file.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Classification of a statement range
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeKind {
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

impl RangeKind {
    /// Returns the table name if this is a table-specific statement
    pub fn table(&self) -> Option<&str> {
        match self {
            RangeKind::Insert(t)
            | RangeKind::DumpingData(t)
            | RangeKind::Lock(t)
            | RangeKind::DisableKeys(t)
            | RangeKind::EnableKeys(t) => Some(t),
            _ => None,
        }
    }

    /// Returns true if this kind can be grouped with INSERT statements for the same table
    pub fn is_table_data(&self) -> bool {
        matches!(
            self,
            RangeKind::Insert(_)
                | RangeKind::DumpingData(_)
                | RangeKind::Lock(_)
                | RangeKind::Unlock
                | RangeKind::DisableKeys(_)
                | RangeKind::EnableKeys(_)
        )
    }
}

/// A contiguous range of bytes with the same classification
#[derive(Debug)]
pub struct FileRange {
    /// Start byte offset (inclusive)
    pub start: u64,
    /// End byte offset (exclusive)
    pub end: u64,
    /// Classification of this range
    pub kind: RangeKind,
}

/// Information about a single statement
struct StatementInfo {
    kind: RangeKind,
    start: u64,
    end: u64,
}

/// Discover ranges in a MySQL dump file using interpolation search.
///
/// Returns ranges classified as `Insert(table_name)` or `Other`.
/// Adjacent ranges of the same kind are merged.
///
/// # Algorithm
///
/// Uses interpolation search to find statement boundaries:
/// 1. Classify first and last statements
/// 2. Find gaps between known statements
/// 3. Sample midpoint of each gap to discover new statements
/// 4. Repeat until no gaps remain (all statements are adjacent)
///
/// When both endpoints and midpoint of a range have the same classification,
/// the entire range is considered homogeneous (no further sampling needed).
pub fn discover_ranges(filepath: &Path) -> Result<Vec<FileRange>, anyhow::Error> {
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
fn classify_position(
    file: &mut File,
    pos: u64,
    file_size: u64,
) -> Result<StatementInfo, anyhow::Error> {
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
fn find_statement_start(file: &mut File, pos: u64) -> Result<u64, anyhow::Error> {
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
fn find_statement_end(file: &mut File, pos: u64, file_size: u64) -> Result<u64, anyhow::Error> {
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
/// Handles both backtick-quoted and unquoted table names.
fn extract_insert_table(after_insert: &str) -> Option<String> {
    let text = after_insert.trim_start();
    let text = text.strip_prefix("INTO")?;
    let text = text.trim_start();

    extract_table_name(text)
}

/// Extract table name from the part after "LOCK TABLES" keyword.
/// Format: " `table_name` WRITE" or " table_name WRITE"
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
    // Pattern: /*!40000 ALTER TABLE `table_name` DISABLE KEYS */
    // or similar with different numbers
    if !text.starts_with("/*!") {
        return None;
    }

    // Check if it contains ALTER TABLE and the key action (DISABLE/ENABLE KEYS)
    if !text.contains("ALTER TABLE") {
        return None;
    }

    let keys_pattern = format!("{} KEYS", key_action);
    if !text.contains(&keys_pattern) {
        return None;
    }

    // Find ALTER TABLE and extract table name after it
    let alter_pos = text.find("ALTER TABLE")?;
    let after_alter = &text[alter_pos + 11..]; // Skip "ALTER TABLE"
    let trimmed = after_alter.trim_start();

    extract_table_name(trimmed)
}

/// Extract table name from "-- Dumping data for table `table_name`" comment
fn extract_dumping_data_comment(text: &str) -> Option<String> {
    // Pattern: "-- Dumping data for table `table_name`"
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
            // Extend current range
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
pub fn merge_table_data(ranges: Vec<FileRange>) -> Vec<FileRange> {
    if ranges.is_empty() {
        return ranges;
    }

    let mut merged = Vec::with_capacity(ranges.len());
    let mut iter = ranges.into_iter().peekable();

    while let Some(current) = iter.next() {
        // Check if this starts a table data sequence
        // Can start with DumpingData comment or Lock
        let (table, start, mut end) = match &current.kind {
            RangeKind::DumpingData(t) => (t.clone(), current.start, current.end),
            RangeKind::Lock(t) => (t.clone(), current.start, current.end),
            _ => {
                merged.push(current);
                continue;
            }
        };

        // Consume all following ranges that belong to this table's data
        // Expected sequence: DumpingData? -> Lock -> DisableKeys -> Insert* -> EnableKeys -> Unlock
        while let Some(next) = iter.peek() {
            if next.start != end {
                break; // Gap between ranges
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
                    break; // UNLOCK ends the sequence
                }
                _ => break,
            }
        }

        merged.push(FileRange {
            start,
            end,
            kind: RangeKind::Insert(table), // Use Insert as the merged kind
        });
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempdir::TempDir;

    fn create_test_file(content: &str) -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new("range_test").unwrap();
        let path = temp_dir.path().join("test.sql");
        let mut file = File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        (temp_dir, path)
    }

    #[test]
    fn test_single_insert() {
        let content = "INSERT INTO `users` VALUES (1,'John');\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].kind, RangeKind::Insert("users".to_string()));
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, content.len() as u64);
    }

    #[test]
    fn test_multiple_tables() {
        let content = "\
INSERT INTO `users` VALUES (1,'John');\n\
INSERT INTO `users` VALUES (2,'Jane');\n\
INSERT INTO `orders` VALUES (1,1);\n\
INSERT INTO `orders` VALUES (2,2);\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].kind, RangeKind::Insert("users".to_string()));
        assert_eq!(ranges[1].kind, RangeKind::Insert("orders".to_string()));
    }

    #[test]
    fn test_mixed_statements() {
        let content = "\
CREATE TABLE `users` (id INT);\n\
INSERT INTO `users` VALUES (1);\n\
INSERT INTO `users` VALUES (2);\n\
UNLOCK TABLES;\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].kind, RangeKind::Other); // CREATE TABLE
        assert_eq!(ranges[1].kind, RangeKind::Insert("users".to_string()));
        assert_eq!(ranges[2].kind, RangeKind::Unlock); // UNLOCK TABLES
    }

    #[test]
    fn test_empty_file() {
        let content = "";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_extract_insert_table_backticks() {
        assert_eq!(
            extract_insert_table(" INTO `my_table` VALUES"),
            Some("my_table".to_string())
        );
    }

    #[test]
    fn test_extract_insert_table_no_backticks() {
        assert_eq!(
            extract_insert_table(" INTO my_table VALUES"),
            Some("my_table".to_string())
        );
    }

    #[test]
    fn test_large_homogeneous_range() {
        // Test that many INSERT statements for same table are detected as one range
        let mut content = String::new();
        for i in 0..1000 {
            content.push_str(&format!("INSERT INTO `users` VALUES ({i},'user{i}');\n"));
        }
        let (_dir, path) = create_test_file(&content);

        let ranges = discover_ranges(&path).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].kind, RangeKind::Insert("users".to_string()));
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, content.len() as u64);
    }

    #[test]
    fn test_many_tables_sequential() {
        // Multiple tables with many INSERTs each
        let mut content = String::new();
        for table in ["alpha", "beta", "gamma", "delta", "epsilon"] {
            for i in 0..100 {
                content.push_str(&format!("INSERT INTO `{table}` VALUES ({i});\n"));
            }
        }
        let (_dir, path) = create_test_file(&content);

        let ranges = discover_ranges(&path).unwrap();
        assert_eq!(ranges.len(), 5);
        assert_eq!(ranges[0].kind, RangeKind::Insert("alpha".to_string()));
        assert_eq!(ranges[1].kind, RangeKind::Insert("beta".to_string()));
        assert_eq!(ranges[2].kind, RangeKind::Insert("gamma".to_string()));
        assert_eq!(ranges[3].kind, RangeKind::Insert("delta".to_string()));
        assert_eq!(ranges[4].kind, RangeKind::Insert("epsilon".to_string()));
    }

    #[test]
    fn test_realistic_dump_structure() {
        let content = "\
-- MySQL dump\n\
SET NAMES utf8mb4;\n\
DROP TABLE IF EXISTS `users`;\n\
CREATE TABLE `users` (id INT, name VARCHAR(255));\n\
LOCK TABLES `users` WRITE;\n\
INSERT INTO `users` VALUES (1,'Alice');\n\
INSERT INTO `users` VALUES (2,'Bob');\n\
INSERT INTO `users` VALUES (3,'Charlie');\n\
UNLOCK TABLES;\n\
DROP TABLE IF EXISTS `orders`;\n\
CREATE TABLE `orders` (id INT, user_id INT);\n\
LOCK TABLES `orders` WRITE;\n\
INSERT INTO `orders` VALUES (1,1);\n\
INSERT INTO `orders` VALUES (2,2);\n\
UNLOCK TABLES;\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();

        // Count INSERT ranges
        let insert_ranges: Vec<_> = ranges
            .iter()
            .filter(|r| matches!(&r.kind, RangeKind::Insert(_)))
            .collect();
        assert_eq!(insert_ranges.len(), 2);
        assert_eq!(
            insert_ranges[0].kind,
            RangeKind::Insert("users".to_string())
        );
        assert_eq!(
            insert_ranges[1].kind,
            RangeKind::Insert("orders".to_string())
        );
    }

    #[test]
    fn test_only_other_statements() {
        let content = "\
CREATE TABLE `users` (id INT);\n\
CREATE TABLE `orders` (id INT);\n\
SET NAMES utf8;\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].kind, RangeKind::Other);
    }

    #[test]
    fn test_merge_table_data() {
        let content = "\
CREATE TABLE `users` (id INT);\n\
LOCK TABLES `users` WRITE;\n\
INSERT INTO `users` VALUES (1);\n\
INSERT INTO `users` VALUES (2);\n\
UNLOCK TABLES;\n\
CREATE TABLE `orders` (id INT);\n\
LOCK TABLES `orders` WRITE;\n\
INSERT INTO `orders` VALUES (1);\n\
UNLOCK TABLES;\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();
        let merged = merge_table_data(ranges);

        // Should have: Other (CREATE), INSERT(users), Other (CREATE), INSERT(orders)
        assert_eq!(merged.len(), 4);
        assert_eq!(merged[0].kind, RangeKind::Other);
        assert_eq!(merged[1].kind, RangeKind::Insert("users".to_string()));
        assert_eq!(merged[2].kind, RangeKind::Other);
        assert_eq!(merged[3].kind, RangeKind::Insert("orders".to_string()));

        // Verify the merged INSERT ranges include LOCK and UNLOCK bytes
        // Original: LOCK (27 bytes) + INSERTs + UNLOCK (15 bytes)
        assert!(merged[1].end - merged[1].start > 60); // More than just INSERTs
    }

    #[test]
    fn test_disable_enable_keys() {
        let content = "\
LOCK TABLES `users` WRITE;\n\
/*!40000 ALTER TABLE `users` DISABLE KEYS */;\n\
INSERT INTO `users` VALUES (1);\n\
/*!40000 ALTER TABLE `users` ENABLE KEYS */;\n\
UNLOCK TABLES;\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();

        // Should detect: Lock, DisableKeys, Insert, EnableKeys, Unlock
        assert_eq!(ranges.len(), 5);
        assert_eq!(ranges[0].kind, RangeKind::Lock("users".to_string()));
        assert_eq!(ranges[1].kind, RangeKind::DisableKeys("users".to_string()));
        assert_eq!(ranges[2].kind, RangeKind::Insert("users".to_string()));
        assert_eq!(ranges[3].kind, RangeKind::EnableKeys("users".to_string()));
        assert_eq!(ranges[4].kind, RangeKind::Unlock);

        // When merged, should become a single INSERT range
        let merged = merge_table_data(ranges);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].kind, RangeKind::Insert("users".to_string()));
    }

    #[test]
    fn test_dumping_data_comment() {
        // Note: The comment lines and LOCK get merged into one statement
        // because comments don't end with ';'
        let content = "\
-- Dumping data for table `users`\n\
LOCK TABLES `users` WRITE;\n\
INSERT INTO `users` VALUES (1);\n\
UNLOCK TABLES;\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();

        // The comment + LOCK is one statement, detected as DUMPING
        assert!(ranges.len() >= 3);
        assert_eq!(ranges[0].kind, RangeKind::DumpingData("users".to_string()));

        // When merged, should become a single INSERT range
        let merged = merge_table_data(ranges);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].kind, RangeKind::Insert("users".to_string()));
    }

    #[test]
    fn test_full_mysqldump_structure() {
        let content = "\
SET NAMES utf8mb4;\n\
CREATE TABLE `users` (id INT);\n\
--\n\
-- Dumping data for table `users`\n\
--\n\
LOCK TABLES `users` WRITE;\n\
/*!40000 ALTER TABLE `users` DISABLE KEYS */;\n\
INSERT INTO `users` VALUES (1);\n\
/*!40000 ALTER TABLE `users` ENABLE KEYS */;\n\
UNLOCK TABLES;\n";
        let (_dir, path) = create_test_file(content);

        let ranges = discover_ranges(&path).unwrap();
        let merged = merge_table_data(ranges);

        // Should have: Other (SET + CREATE), INSERT(users)
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].kind, RangeKind::Other);
        assert_eq!(merged[1].kind, RangeKind::Insert("users".to_string()));
    }
}

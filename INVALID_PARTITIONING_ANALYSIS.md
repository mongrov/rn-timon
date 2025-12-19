# Invalid Partitioning Error - Root Cause Analysis

## Error Context
**Error Message**: `"QueryExecutionFailed: DataFusion operation failed [Source: Arrow error: External error: Execution error: Invalid partitioning found on disk]"`

**When it occurs**: Intermittently when syncing IoT ring data to mobile app (ring → mobile app → Timon insert → query)

## Data Flow Analysis

### 1. Insert Flow (Ring → Mobile App → Timon)

```
IoT Ring → Mobile App → nativeInsert() → Rust insert() → Write Parquet Files
```

**Key Code Path** (`db_manager.rs:352-479`):
1. Mobile app receives data from ring
2. Calls `TimonModule.nativeInsert(dbName, tableName, jsonData)`
3. Rust `insert()` function:
   - Reads existing parquet files from all partitions (line 415-427)
   - For each new record:
     - Calculates partition: `partition_date=YYYY-MM-DD` (line 440)
     - Creates partition directory: `fs::create_dir_all(&partition_dir).ok()` (line 444)
     - Sets target file: `{partition_dir}/data.parquet` (line 445)
   - Writes all updated/new files using `parquet_file_writer()` (line 465-471)

**Critical Operation** (`db_manager.rs:831-840`):
```rust
fn parquet_file_writer(path: &Path, schema: Schema, array: Vec<Arc<dyn Array>>) {
    let file = fs::File::create(&path)?;  // ⚠️ TRUNCATES FILE IMMEDIATELY
    let mut writer = ArrowWriter::try_new(file, ...)?;
    writer.write(&combined_batch)?;
    writer.close()?;  // ⚠️ File only complete after close()
}
```

### 2. Query Flow

```
Mobile App → nativeQuery() → Rust query() → register_single_table() → DataFusion ListingTable
```

**Key Code Path** (`db_manager.rs:481-583`):
1. Mobile app calls `query()`
2. `query()` calls `register_single_table()` for each table (line 517)
3. `register_single_table()` (line 585-690):
   - Checks if parquet files exist (line 609-626)
   - Creates `ListingOptions` with partition columns (line 646-651):
     ```rust
     .with_table_partition_cols(vec![("partition_date".to_string(), DataType::Utf8)])
     ```
   - Infers schema from parquet files (line 657)
   - Creates `ListingTable` with `ListingTable::try_new(config)?` (line 688)
   - **This is where the error occurs** - DataFusion validates partition structure

## Root Causes Identified

### 🔴 **CRITICAL ISSUE #1: Race Condition - File Writing During Query**

**Problem**: 
- `fs::File::create()` **immediately truncates** the target file (line 833)
- File is only complete after `writer.close()` (line 840)
- If a query executes during this window, DataFusion sees:
  - An empty file (0 bytes)
  - A partially written file (corrupted)
  - An incomplete parquet file structure

**Scenario**:
```
Time T0: Insert starts writing to partition_date=2025-01-15/data.parquet
         → fs::File::create() truncates file (file now 0 bytes)
Time T1: Query executes → DataFusion scans partitions
         → Finds partition_date=2025-01-15/ directory
         → Tries to read data.parquet (still being written, 0 bytes or incomplete)
         → ERROR: "Invalid partitioning found on disk"
Time T2: Insert completes → writer.close() → file is now valid
```

**Evidence**:
- No file locking mechanism
- No atomic writes (no temp file + rename pattern)
- `fs::File::create()` is not atomic

### 🔴 **CRITICAL ISSUE #2: Empty Partition Directories**

**Problem**:
- `fs::create_dir_all(&partition_dir).ok()` creates directory even if write fails (line 444)
- If insert fails after creating directory but before writing file, you get:
  ```
  table_name/
    partition_date=2025-01-15/  ← Directory exists
      (empty - no data.parquet)
  ```
- DataFusion expects partition directories to contain valid parquet files
- Empty partition directories cause "Invalid partitioning" error

**Scenario**:
```
1. Insert creates partition_date=2025-01-15/ directory
2. Insert fails (out of memory, disk full, etc.)
3. Directory exists but is empty
4. Query executes → DataFusion sees empty partition directory
5. ERROR: "Invalid partitioning found on disk"
```

### 🔴 **CRITICAL ISSUE #3: Concurrent Inserts to Same Partition**

**Problem**:
- Multiple inserts can target the same partition file simultaneously
- `fs::File::create()` from multiple threads will:
  - Overwrite each other's work
  - Create corrupted files
  - Leave files in inconsistent state

**Scenario**:
```
Thread 1: Insert record A → partition_date=2025-01-15/data.parquet
          → fs::File::create() → starts writing
Thread 2: Insert record B → partition_date=2025-01-15/data.parquet (SAME FILE!)
          → fs::File::create() → TRUNCATES Thread 1's file
          → Starts writing
Thread 1: Continues writing to truncated file → CORRUPTION
Query:   Reads corrupted file → ERROR
```

### 🟡 **ISSUE #4: Schema Inference During Writes**

**Problem**:
- `infer_schema_with_coercion()` reads all parquet files to merge schemas (line 657)
- If files are being written during schema inference:
  - May read incomplete files
  - May read files with wrong schema (if write is mid-way)
  - Causes schema inference to fail or produce wrong schema

### 🟡 **ISSUE #5: No Validation of Written Files**

**Problem**:
- After writing parquet files, there's no validation that:
  - File is complete
  - File is readable
  - File has valid parquet structure
- If write fails silently or partially, corrupted files remain on disk

## DataFusion's Partition Validation

When `ListingTable::try_new()` is called, DataFusion:

1. **Scans the directory structure** looking for Hive-style partitions (`partition_date=YYYY-MM-DD/`)
2. **Validates partition structure**:
   - Each partition directory must contain valid parquet files
   - Partition values must be parseable
   - All partitions must have consistent structure
3. **Reads partition metadata** from parquet files
4. **Fails if**:
   - Empty partition directories found
   - Corrupted parquet files found
   - Inconsistent partition structure
   - Files that don't match expected schema

**The error "Invalid partitioning found on disk" occurs when DataFusion detects any of these issues during the validation phase.**

## When This Error Occurs

Based on the code analysis, the error is most likely to occur when:

1. **Query executes immediately after insert** (race condition)
2. **Insert fails mid-way** (leaves empty partition directory)
3. **Multiple inserts happen concurrently** (file corruption)
4. **Disk space issues** (incomplete writes)
5. **App crash during insert** (partial file writes)

## Evidence in Code

### File Writing (Non-Atomic)
```rust
// db_manager.rs:831-840
fn parquet_file_writer(path: &Path, schema: Schema, array: Vec<Arc<dyn Array>>) {
    let file = fs::File::create(&path)?;  // ⚠️ Non-atomic, truncates immediately
    // ... write data ...
    writer.close()?;  // File only valid after this
}
```

### Directory Creation (No Cleanup on Failure)
```rust
// db_manager.rs:443-445
let partition_dir = format!("{}/partition_date={}", table_path, partition_value);
fs::create_dir_all(&partition_dir).ok();  // ⚠️ Creates even if write fails
let target_file = format!("{}/data.parquet", partition_dir);
```

### No File Locking
- No mutex/lock around file writes
- No file-level locking
- Multiple threads can write to same file

### No Validation After Write
- No check that file is readable after write
- No validation of parquet file integrity
- Corrupted files remain on disk

## Recommendations for Investigation

### 1. Add Logging
Add detailed logging to track:
- When inserts start/complete
- When queries execute
- File write operations
- Partition directory creation
- Any errors during file operations

### 2. Check for Empty Partitions
Add a diagnostic function to find empty partition directories:
```rust
// Check for empty partition directories
fn find_empty_partitions(table_dir: &str) -> Vec<String>
```

### 3. Check File Timestamps
Compare file modification times:
- If query happens within seconds of insert, likely race condition
- If partition directory exists but file doesn't, likely failed insert

### 4. Monitor Concurrent Operations
- Check if multiple inserts happen simultaneously
- Check if queries happen during active inserts
- Use thread/process IDs in logs

### 5. Validate Parquet Files
Add a function to validate all parquet files:
```rust
// Validate all parquet files in a table
fn validate_table_files(table_dir: &str) -> Result<Vec<String>, Error>
```

## Conclusion

The "Invalid partitioning found on disk" error is **most likely caused by**:

1. **Race conditions** - Queries reading files while they're being written (Issue #1)
2. **Empty partition directories** - Failed inserts leaving empty directories (Issue #2)
3. **File corruption** - Concurrent writes to same partition file (Issue #3)

The error is **NOT a bug in DataFusion** - it's correctly detecting invalid partition structures. The issue is in the **Timon library's file writing logic** which:
- Doesn't use atomic writes
- Doesn't prevent concurrent access
- Doesn't clean up on failure
- Doesn't validate after writes

## Next Steps

1. **Add logging** to capture exact timing of inserts and queries
2. **Check filesystem** for empty partition directories and corrupted files
3. **Monitor** for concurrent operations
4. **Implement fixes** (atomic writes, file locking, validation) once root cause is confirmed


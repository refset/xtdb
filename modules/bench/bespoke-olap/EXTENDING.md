# Extending the Bespoke OLAP Engine

Instructions for an LLM session adding new queries or benchmarks to the C++
bespoke OLAP engine. Read this entire document before making changes.

## What This Engine Is

A minimal C++ engine that reads XTDB's Arrow trie files directly, resolves
bitemporality via the Ceiling+Polygon algorithm, and executes hand-tuned
query implementations. It exists as a **diagnostic tool** to measure the
performance floor — what's achievable with XTDB's data format but zero
query-planning overhead. All results are validated against XTDB's own
SQL engine output.

## Architecture Overview

```
main.cpp          — Query registry, timing harness, JSON output
loader.h/cpp      — Catalog parsing, Arrow file I/O, bitemporal resolution, table building
queries.h/cpp     — Per-query implementations (pure compute, no I/O)
types.h           — Table structs (SoA layout), FusionData, QueryParams
arrow_access.h    — Arrow helpers (OpInfo, CachedBatch, FileRef, RowSource, type accessors)
bitemporal.h      — Ceiling + Polygon algorithm, Event/ResolvedRow/IID structs
CMakeLists.txt    — Build config (Arrow, nlohmann_json, protobuf)
```

### Data Flow

```
Catalog JSON → parse_catalog() → CatalogInfo {tables JSON, QueryParams}
                                       ↓
                              load_tables(cat, {"system", "readings"})
                                       ↓
                              For each needed table:
                                1. load_files()      — read Arrow IPC files
                                2. resolve_table()   — extract events, sort, polygon → ResolvedRow[]
                                3. build_<table>()   — read data columns → typed struct
                                       ↓
                              FusionData (all loaded tables)
                                       ↓
                              query_<name>(data, params) → JSON results
```

### Key Design Decisions

1. **Per-query table loading.** Each query declares which tables it needs.
   `load_tables` only loads those. This gives fair per-query timing vs XTDB.

2. **Struct-of-Arrays (SoA) layout.** Table structs use parallel vectors
   (`std::vector<std::string> id`, `std::vector<Timestamp> valid_from`, etc.)
   not row objects. This enables cache-friendly sequential scans.

3. **Results as JSON.** Queries return `nlohmann::json` arrays. The Clojure
   harness compares these against XTDB query output field-by-field.

## How to Add a New Query

### Step 1: Understand the SQL

Get the XTDB SQL query you're implementing. Note which tables it reads,
what joins/aggregations it performs, and what the output columns are.
Pay attention to `FOR ALL VALID_TIME` — without it, XTDB applies temporal
CONTAINS filters that change result semantics.

### Step 2: Check if needed tables exist

Look at `types.h` for existing table structs. If your query uses a table
not yet defined, add it following the existing patterns:

```cpp
// types.h — SoA pattern: parallel vectors, index maps
struct MyTable {
    std::unordered_map<std::string, int32_t> id_index;  // _id → row index
    std::vector<std::string> id;
    std::vector<Timestamp> valid_from;
    std::vector<Timestamp> valid_to;
    std::vector<std::string> some_field;
    std::vector<double> some_metric;
};
```

Add it to `FusionData`:
```cpp
struct FusionData {
    // ... existing tables ...
    MyTable my_table;
};
```

### Step 3: Add table loading in loader.cpp

For small tables (< 10K rows), use the `build_simple_table` template with
`CachedBatch` — it handles DenseUnion offset indirection automatically:

```cpp
if (needed.count("my_table")) {
    auto [files, rows] = load_and_resolve("my_table");
    build_simple_table(data.my_table, files, rows,
        [](MyTable& t, CachedBatch& cb, int64_t row, int64_t vf, int64_t vt) {
            auto id = cb.str("_id", row);
            t.id_index[id] = static_cast<int32_t>(t.id.size());
            t.id.push_back(id);
            t.valid_from.push_back(vf);
            t.valid_to.push_back(vt);
            t.some_field.push_back(cb.str("some_field", row));
            t.some_metric.push_back(cb.f64("some_metric", row));
        });
}
```

For large tables (100K+ rows), see the optimization tiers below.

### Step 4: Implement the query

In `queries.h`, declare the function:
```cpp
json query_my_query(const FusionData& data, const QueryParams& params);
```

In `queries.cpp`, implement it. Focus on correctness first — match XTDB's
output exactly. Optimize only after validation passes.

### Step 5: Register the query

In `main.cpp`, add to the `queries` vector:
```cpp
{"my-query", {"my_table", "system"}, query_my_query},
```

The second element lists which tables this query needs. Only those tables
will be loaded and included in the timing.

### Step 6: Add to the Clojure harness

In `fusion_bespoke.clj`:

1. Add the XTDB SQL query as a `def`
2. Add it to `run-xtdb-queries`
3. Add a comparison case to `print-comparison`

### Step 7: Add query params if needed

If your query needs parameters beyond `sample_system_id`, `min_valid_time`,
`max_valid_time`, extend `QueryParams` in `types.h` and `parse_catalog` in
`loader.cpp`. Update `write-catalog` in the Clojure harness to emit them.

## Optimization Tiers

These tiers were discovered empirically. Apply them in order — each builds
on the previous.

### Tier 0: CachedBatch (small tables, < 10K rows)

Use `build_simple_table` with `CachedBatch`. This caches Arrow field arrays
per batch and handles DenseUnion offset indirection. Good enough for
reference tables (site, device, test_suite, etc.).

```cpp
// CachedBatch usage — handles DenseUnion transparently
CachedBatch cb;
cb.ensure(files, source_file, source_batch);
std::string val = cb.str("field_name", source_row);
int64_t ts = cb.i64("timestamp_field", source_row);
double d = cb.f64("numeric_field", source_row);
```

### Tier 1: Zero-copy with RowSource (wide tables, few accessed columns)

If a table has many columns but queries only touch a few, materialize only
the commonly-used columns and store `RowSource` references for on-demand
Arrow access. The `system` table uses this pattern — 5 columns materialized,
28 accessed from Arrow only when needed (system-settings query).

```cpp
// types.h
struct WideTable {
    std::vector<std::string> id;           // materialized — used by all queries
    std::vector<Timestamp> valid_from;     // materialized
    std::vector<FileRef> files;            // Arrow file references for on-demand access
    std::vector<RowSource> sources;        // per-row (file, batch, row) for on-demand access
};

// queries.cpp — on-demand access for rarely-used columns
auto& src = data.wide_table.sources[idx];
CachedBatch cb;
cb.ensure(data.wide_table.files, src.file, src.batch);
double rare_field = cb.f64("rare_field", src.row);
```

### Tier 2: Batch-level raw pointers (large tables, hot loop)

For tables with 100K+ rows where build time matters, extract typed array
pointers once per batch and use direct pointer arithmetic per row. This
eliminates per-row hash map lookups and type dispatch.

```cpp
// loader.cpp — batch-level extraction pattern
int cur_fi = -1, cur_bi = -1;
const int64_t* ts_raw = nullptr;
const double* val_raw = nullptr;
const int32_t* put_offsets = nullptr;

for (auto& rr : rows) {
    if (rr.source_file != cur_fi || rr.source_batch != cur_bi) {
        cur_fi = rr.source_file;
        cur_bi = rr.source_batch;
        auto& batch = files[cur_fi].arrow_file.batches[cur_bi];
        auto op_col = col(batch, "op");
        auto union_arr = std::static_pointer_cast<arrow::DenseUnionArray>(op_col);
        auto put_struct = std::static_pointer_cast<arrow::StructArray>(union_arr->field(0));
        put_offsets = union_arr->raw_value_offsets();
        ts_raw = raw_i64(put_struct->GetFieldByName("timestamp_col"));
        val_raw = raw_f64(put_struct->GetFieldByName("value_col"));
    }
    int32_t off = put_offsets[rr.source_row];
    // Direct pointer access — no hash maps, no type checks
    my_timestamps[i] = ts_raw[off];
    my_values[i] = val_raw ? val_raw[off] : 0.0;
}
```

Key helpers in `loader.cpp`:
- `raw_i64(array)` → `const int64_t*` (handles Timestamp and Int64)
- `raw_f64(array)` → `const double*` (handles Double)
- `read_iid(arr, i)` → `IID` from FixedSizeBinary(16)

### Tier 3: Fused resolve+build (large tables, sort-dominated)

For the largest tables, fuse bitemporal resolution with data extraction.
Instead of resolve → ResolvedRow[] → build, extract data columns inline
during polygon resolution when batch data is cache-hot.

This is what `resolve_and_build_readings()` does. The key technique:

1. Pre-cache data column arrays for all (file, batch) pairs
2. During polygon resolve, when a range is produced, immediately extract
   data columns from the event's batch (already in cache)
3. Build the output structures in one pass

```cpp
// Pre-cache pattern
struct BatchDataCache {
    const int32_t* put_offsets = nullptr;
    bool has_union = false;
    std::shared_ptr<arrow::StringArray> group_col_str;
    const double* value_raw = nullptr;
};

std::vector<std::vector<BatchDataCache>> batch_data(files.size());
// ... populate during event extraction ...

// During polygon resolve — data access is cache-hot
auto& bd = batch_data[ev.source_file][ev.source_batch];
int32_t off = bd.has_union ? bd.put_offsets[ev.source_row] : ev.source_row;
std::string_view group = bd.group_col_str->GetView(off);
double val = bd.value_raw[off];
```

This eliminated ~130ms from the readings pipeline (200K rows).

### Tier 4: Integer group IDs for sorting

If you need to sort by a string column (e.g., group-by system_id), assign
integer group IDs during extraction and sort by integer instead of string.
Reduces sort comparison from ~20-char memcmp to single int comparison.

```cpp
std::unordered_map<std::string_view, int32_t> group_map;
std::vector<std::string_view> group_list;

// During extraction:
auto [it, inserted] = group_map.try_emplace(sv, static_cast<int32_t>(group_list.size()));
if (inserted) group_list.push_back(sv);
int32_t group_id = it->second;

// Sort by integer — 3-4x faster than string sort
std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
    if (a.group_id != b.group_id) return a.group_id < b.group_id;
    return a.valid_from < b.valid_from;
});
```

## Arrow Format Pitfalls

These caused real bugs. Read them all.

1. **Data columns are inside `op.put`, NOT top-level.** The `op` column is a
   DenseUnion. Child 0 ("put") is a Struct containing all data columns.
   Don't look for `site_id` at `batch->GetColumnByName("site_id")` — it's at
   `op_union->field(0)->GetFieldByName("site_id")`.

2. **DenseUnion offset indirection.** Row N in the batch does NOT map to
   index N in the put struct. You MUST use `union_arr->value_offset(row)` or
   `union_arr->raw_value_offsets()[row]` to get the correct index into put's
   child arrays.

3. **String types vary between files.** The same column can be `STRING` in
   one file and `LARGE_STRING` in another (especially after compaction).
   Always handle both:
   ```cpp
   if (arr->type_id() == arrow::Type::STRING)
       return std::static_pointer_cast<arrow::StringArray>(arr)->GetView(i);
   else if (arr->type_id() == arrow::Type::LARGE_STRING)
       return std::static_pointer_cast<arrow::LargeStringArray>(arr)->GetView(i);
   ```

4. **Timestamps are microseconds since epoch.** Not milliseconds, not
   nanoseconds. All temporal values in XTDB are microseconds.

5. **`INT64_MAX` means "forever" / "not overridden".** `_valid_to = INT64_MAX`
   means valid until end of time. `system_to = INT64_MAX` in polygon output
   means visible in current snapshot.

6. **Ceiling reset order.** `valid_times` must initialize to
   `[INT64_MAX, INT64_MIN]` (descending). Using ascending order produces
   subtly wrong bitemporal resolution.

7. **Multiple batches per file.** Arrow IPC files can contain multiple
   record batches. Always iterate `reader->num_record_batches()`.

8. **`is_put` check via type codes.** The fast way to check if a row is a
   put operation: `raw_type_codes()[row] == 0`. Type code 0 = put,
   1 = delete, 2 = erase. Use `raw_type_codes()` from the DenseUnionArray
   to avoid per-row virtual dispatch.

## Bitemporal Resolution

Every table must go through bitemporal resolution before querying. The
algorithm is in `bitemporal.h` (Ceiling + Polygon). Do not skip this
step even if a table appears append-only — the Clojure harness may add
updates during benchmark setup.

### resolve_table() pipeline

1. **Extract events** from all Arrow files/batches — `_iid`, `_system_from`,
   `_valid_from`, `_valid_to`, `is_put`, source references
2. **Sort** by `(iid ASC, system_from DESC)` — newest tx first per entity
3. **Polygon resolve** — walk events grouped by IID:
   - Reset ceiling on IID change
   - For delete/erase: apply to ceiling, skip remaining events for this IID
   - For put: calculate polygon, apply to ceiling, emit resolved ranges
   - Keep ranges where `system_to == INT64_MAX` and `valid_from != valid_to`

Output: `vector<ResolvedRow>` with `{valid_from, valid_to, source_file, source_row, source_batch}`

### Current snapshot filter

For all current work we only care about the current snapshot:
```cpp
if (pr.valid_from != pr.valid_to && pr.system_to == INT64_MAX) {
    // This range is visible now — keep it
}
```

## Build and Run

### Prerequisites

- CMake 3.16+, C++17 compiler
- Apache Arrow C++ (`libarrow-dev` or equivalent)
- nlohmann-json (`nlohmann-json3-dev`)
- protobuf (`libprotobuf-dev`)

### Build

```bash
cd modules/bench/bespoke-olap/build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run standalone (requires catalog JSON from a previous Clojure run)

```bash
./bespoke_olap /tmp/fusion-bespoke-catalog.json
```

Output: JSON to stdout with query results and latencies. Profiling to stderr.

### Run full benchmark (generates data, runs XTDB, runs C++, compares)

```bash
cd /path/to/xtdb2
java @/tmp/xtdb-args2.txt -e \
  "(require 'xtdb.bench.fusion-bespoke) (xtdb.bench.fusion-bespoke/run-fusion-bespoke)"
```

This:
1. Creates XTDB node, populates Fusion benchmark data
2. Runs compaction (including cross-recency L2H)
3. Writes catalog JSON
4. Runs XTDB queries (3 iterations, reports median)
5. Runs C++ engine (3 iterations internally)
6. Compares results field-by-field, prints speedup table

### Helper scripts

- `build/build.sh` — build only
- `build/run-cpp.sh` — run C++ engine, stderr to `/tmp/cpp-stderr.txt`
- `build/run-bench.sh` — full benchmark (Clojure + C++)

## Profiling

### Built-in timing

The engine prints phase-level timing to stderr automatically:
```
resolve system: 5095 raw -> 5095 resolved [extract=400us sort=600us resolve=700us]
system: io=2500us resolve=1800us
build system: 6000us
```

For the fused readings pipeline:
```
readings: io=17000us
resolve+build readings: 200000 raw -> 200000 resolved
  phases: extract=8000us sort_events=35000us resolve+data=50000us sort_entries=44000us write=3000us
```

### Adding profiling to a new section

```cpp
auto t0 = std::chrono::high_resolution_clock::now();
// ... work ...
auto t1 = std::chrono::high_resolution_clock::now();
auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
std::cerr << "    my_phase: " << us << "us\n";
```

## Performance Reference (1000 devices, 200K readings, post-compaction)

| Query | XTDB (ms) | C++ (ms) | Speedup |
|-------|-----------|----------|---------|
| system-settings (1 row) | 31 | 12 | 2.5x |
| readings-for-system (1194 rows) | 494 | 183 | 2.7x |
| system-count-over-time (17 rows) | 384 | 11 | 35.3x |
| readings-range-bins (17 rows) | 1564 | 173 | 9.0x |
| cumulative-registration (34 rows) | 1480 | 49 | 30.3x |

### Where time goes (readings pipeline, 200K rows)

| Phase | Time |
|-------|------|
| File I/O | 17ms |
| Extract events | 8ms |
| Sort events (iid, system_from) | 35ms |
| Polygon resolve + data extract | 50ms |
| Sort entries (group_id, valid_from) | 44ms |
| Write output arrays | 3ms |
| **Total** | **157ms** |

The two sorts (79ms) are 50% of the pipeline. Further gains require
algorithmic changes (k-way merge, radix sort) or pre-sorted storage.

## Checklist for Adding a Query

- [ ] SQL query understood, including temporal semantics
- [ ] Table structs exist in `types.h` (or added)
- [ ] Table loading added in `loader.cpp` (with appropriate optimization tier)
- [ ] Query function in `queries.cpp` returns correct JSON
- [ ] Query declared in `queries.h`
- [ ] Query registered in `main.cpp` with correct table set
- [ ] Clojure harness updated: SQL def, `run-xtdb-queries`, `print-comparison`
- [ ] `build.sh` passes
- [ ] `run-bench.sh` shows MATCH for the new query
- [ ] Profiling output reviewed — no unexpected hotspots

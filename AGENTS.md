# Agents Guide for XTDB2

## Clojure REPL Access via clojure-mcp

### Setup (if not already configured)

Project has `deps.edn` with clojure-mcp alias. If MCP not added to Claude Code yet:

```bash
claude mcp add --transport stdio --scope project xtdb-clojure-repl -- clojure -X:mcp :port 7888 :project-dir '"/home/jdt/ghq/github.com/xtdb/xtdb2"'
```

**Note**: Replace the project path with your actual XTDB2 repository location.

### Starting the REPL

Two terminals required:

**Terminal 1** - Start nREPL (gradle clojureRepl task with CIDER middleware):
```bash
./gradlew clojureRepl -PreplPort=7888
```

**Terminal 2** - Start clojure-mcp (connects to nREPL on port 7888):
```bash
clojure -X:mcp :port 7888 :project-dir '"<absolute-path-to-xtdb2>"'
```

Note: `deps.edn` has a default `:project-dir` configured, but you may need to override it if running from a different location.

### Usage

Use clojure-mcp tools to:
- Evaluate Clojure code: `(+ 1 2)`
- Require test namespaces: `(require 'xtdb.api-test :reload)`
- Run tests: `(clojure.test/run-tests 'xtdb.api-test)`
- Run specific test: `(clojure.test/test-var #'xtdb.api-test/test-name)`

### When to Use clojure-mcp

**Particularly useful for**: Adding scalar functions to SQL standard library
- Functions should match PostgreSQL behavior first and foremost
- Tests typically in `src/test/clojure/xtdb/expression_test.clj` and `src/test/clojure/xtdb/sql/expr_test.clj`
- **Documentation must be updated** in `docs/src/content/docs/reference/main/stdlib/*.md` (e.g., `numeric.md`, `string.md`, `temporal.md`)
- ANTLR grammar definitions need updating alongside function implementation
- **Workflow**: Make Clojure changes and test iteratively in live REPL session (don't restart unnecessarily)

### Test Locations & Organization

Tests organized by functionality:

**Core API & Node**
- `api_test.clj` - Core API functionality
- `node_test.clj` - Node lifecycle and operations
- `database_test.clj` - Database operations
- `main_test.clj` - Main entry point
- `healthz_test.clj` - Health check endpoints

**Expression System**
- `expression_test.clj` - Scalar functions, operators (primary location for new SQL functions)
- `expression/temporal_test.clj` - Temporal expression functions
- `expression/list_test.clj` - List/array expression functions
- `expression/uri_test.clj` - URI functions

**SQL**
- `sql_test.clj` - SQL query execution
- `sql/expr_test.clj` - SQL expressions
- `sql/temporal_test.clj` - SQL temporal queries
- `sql/interval_test.clj` - SQL interval types
- `sql/generate_series_test.clj` - Generate series function
- `sql/with_test.clj` - WITH/CTE queries
- `sql/multi_db_test.clj` - Multi-database queries
- `sql/logic_test/*` - SQLite Logic Tests (SLT)

**XTQL**
- `xtql_test.clj` - XTQL query language
- `xtql/plan_test.clj` - XTQL query planning
- `xtql/temporal_test.clj` - XTQL temporal queries

**Query Operators** (`operator/*_test.clj`)
- `scan_test.clj`, `join_test.clj`, `select_test.clj`, `project_test.clj`
- `group_by_test.clj`, `order_by_test.clj`, `window_test.clj`
- `apply_test.clj`, `let_test.clj`, `set_test.clj`, `distinct_test.clj`
- `unnest_test.clj`, `patch_test.clj`, `external_data_test.clj`

**Temporal Features**
- `as_of_test.clj` - AS OF temporal queries
- `default_tz_test.clj` - Default timezone handling
- `expression/temporal_test.clj`, `sql/temporal_test.clj`, `xtql/temporal_test.clj`

**Storage & Data Structures**
- `arrow_test.clj` - Arrow integration
- `arrow_edn_test.clj` - Arrow EDN serialization
- `vector_test.clj`, `vector/reader_test.clj`, `vector/writer_test.clj`
- `trie_test.clj`, `trie_catalog_test.clj` - Hash trie structures
- `buffer_pool_test.clj` - Memory buffer management
- `object_store_test.clj` - Object storage abstraction
- `block_boundary_test.clj` - Block boundaries
- `metadata_test.clj` - Metadata handling

**Indexing & Compaction**
- `indexer_test.clj` - Main indexer
- `indexer/live_index_test.clj`, `indexer/live_table_test.clj`
- `compactor_test.clj` - Compactor
- `compactor/segment_merge_test.clj`, `compactor/reset_test.clj`
- `segment/merge_plan_test.clj`

**PgWire Protocol**
- `pgwire_test.clj` - Main PgWire tests
- `pgwire_protocol_test.clj` - Protocol-level tests
- `pgwire/copy_test.clj` - COPY command
- `pgwire/types_test.clj` - Type mappings
- `pgwire/dbeaver_test.clj`, `pgwire/pg2_test.clj` - Client compatibility
- `pgwire/playground_test.clj`

**External Integrations**
- `aws/s3_test.clj`, `aws/minio_test.clj` - S3 storage
- `azure_test.clj` - Azure Blob storage
- `gcp_test.clj` - Google Cloud Storage
- `kafka_test.clj` - Kafka log
- `next/jdbc_test.clj` - JDBC integration

**Authentication & Security**
- `authn_test.clj` - Authentication
- `oidc_integration_test.clj` - OIDC integration

**Specialized Tests**
- `query_test.clj` - Query execution
- `logical_plan_test.clj` - Query planning
- `types_test.clj` - Type system
- `information_schema_test.clj` - Information schema
- `table_catalog_test.clj` - Table catalog
- `log_test.clj` - Transaction log
- `stats_test.clj` - Statistics
- `metrics_test.clj` - Metrics collection

**Benchmark & Data Tests**
- `tpch_test.clj` - TPC-H benchmark
- `ts_devices_small_test.clj` - Time series devices
- `bench/auctionmark_test.clj` - AuctionMark benchmark

**Utilities**
- `util_test.clj` - Utilities
- `issue_test.clj` - Specific issue reproductions
- `lint_test.clj` - Code linting
- `docker_test.clj` - Docker integration

### Key Constraints

- **Clojure changes**: Always use live REPL - reload with `:reload` flag, test immediately
- **Kotlin/Java changes**: Must rebuild and restart REPL to recompile
- **Integration tests**: May need Docker (Kafka, MinIO, etc.)

### Gradle clojureRepl Flags

- `-PdebugJvm`: Remote debugging on port 5005
- `-PtwelveGBJvm`: 12GB memory (default is 6GB)
- `-PnoLocalsClearing`: Better debugging
- `-ParrowUnsafeMemoryAccess`: Enable Arrow unsafe memory

---

## Key File Locations

### SQL & Expression System
- SQL grammar: `core/src/main/antlr/xtdb/antlr/Sql.g4`, `SqlLexer.g4`
- Expression implementation: `core/src/main/clojure/xtdb/expression.clj`
- Expression tests: `src/test/clojure/xtdb/expression_test.clj`
- SQL parser: `core/src/main/clojure/xtdb/sql.clj`
- SQL tests: `src/test/clojure/xtdb/sql_test.clj`

### Query Operators
- Implementations: `core/src/main/clojure/xtdb/operator/*.clj` (scan, join, project, etc.)
- Tests: `src/test/clojure/xtdb/operator/*_test.clj`

### Storage & Serialization
- Protobuf definitions: `api/src/main/proto/`, `core/src/main/proto/`
- Storage layer: `core/src/main/kotlin/xtdb/` (Kotlin)

### Module Structure
- `api/` - Public API (Clojure + Kotlin)
- `core/` - Database engine (Clojure + Kotlin + Protobuf + ANTLR)
- `modules/` - Kafka, AWS, Azure, GCP, Flight SQL, Kafka Connect

---

## Dev Namespace Workflow

Located in `src/dev/clojure/dev.clj` - Integrant-based REPL workflow:

- `(dev)` - Switch to dev namespace
- `(go)` - Start dev XTDB node
- `(halt)` - Stop node
- `(reset)` - Stop, reload changed namespaces, restart
- `dev/node` - Access running node: `(xt/status node)`

**REPL restart required for**: Adding modules, dependency changes, Java/Kotlin changes
**REPL restart NOT needed for**: Clojure changes (use `(reset)`)

---

## Test Types & Execution

- Unit tests: `./gradlew test` (excludes integration/property/docker tags)
- Integration tests: `./gradlew integration-test` (12GB memory)
- Property tests: `./gradlew property-test -Piterations=100`
- Single test file: `./gradlew test --tests 'xtdb.api_test**'`

**Test tags**: `integration`, `property`, `s3`, `minio`, `kafka`, `docker`, `azure`, `google-cloud`

**External dependencies**: Some tests require `docker-compose up` (Kafka, MinIO, Keycloak)

---

## Codebase Composition

- **Kotlin** (58%): Storage layer, utilities, catalog, database engine
- **Clojure** (42%): Query engine, SQL parsing, operators, PgWire protocol
- **ANTLR**: SQL grammar → generates Java parser
- **Protobuf**: Serialization formats → generates Java classes

**JDK 21 required**

---

## REPL Change Requirements

### Hot reload (no restart)
- Clojure source changes
- Test changes
- Use `:reload` flag when requiring

### Must restart REPL
- Kotlin/Java changes (requires recompilation)
- ANTLR grammar changes (generates Java)
- Protobuf changes (generates Java)
- Dependency changes
- Adding/removing modules

### Gradle rebuild needed (then restart REPL)
- Kotlin code: `./gradlew compileKotlin`
- ANTLR: `./gradlew generateGrammarSource`
- Protobuf: `./gradlew generateProto`

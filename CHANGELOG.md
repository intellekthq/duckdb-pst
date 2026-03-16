# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-03-16

- DuckDB 1.5 (Variegata) compatibility
  - `OptimizerExtension::Register` API change
  - `row_start` replaced with `optional_idx`
  - `FileSystem::HasGlob` context parameter removed
- Windows: fixed CRT mismatch linker error caused by static CRT (`/MT`) switch in `extension-ci-tools`

## [0.1.1] - 2026-02-22

- Windows: fixed MSVC build by adding `bytes_to_string` Windows implementation and resolving `std::byte` conflicts

## [0.1.0] - 2026-02-02

- Finished implementing late materialization optimizer pushdown support
  - Partition-level pruning via `PSTInputPartition::prune()` for both `__partition` and `__node_id` filters
  - Filters applied during global state initialization for optimal performance
  - Added `PSTFilterOptimizer` to prevent pushdown of non virtual column filters and replace `TableFunction::pushdown_complex_filters`
- Implemented table function `pushdown_expression` API / specified no arbitrary expression pushdown
- Optimized message class filtering using `memcmp` with predefined MAPI type constants instead of string comparisons
- Updated cardinality statistics to reflect actual files and rows read (not just planned) during `EXPLAIN ANALYZE`
- macOS: fixed iconv UTF-16LE decoding bug by explicitly linking GNU libiconv instead of system iconv
- Added `planning_concurrency` table function parameter

## [0.0.1] - 2025-12-23

- Initial release of duckdb-pst extension
- Table functions for reading PST files:
  - `read_pst_folders`
  - `read_pst_messages`
  - `read_pst_notes`
  - `read_pst_contacts`
  - `read_pst_appointments`
  - `read_pst_sticky_notes`
  - `read_pst_tasks`
  - `read_pst_distribution_lists`
- Planning features:
  - Parallel table scanning
  - Partition-based reads using PST NDB node batching
  - Column pruning via projection pushdown
  - Statistics pushdown for efficient `COUNT(*)` queries
  - Cardinality estimation based on partition statistics
  - Concurrent partition planning for directories with many PST files
- Object storage support/integration with DuckDB's `FileSystem` abstraction

### Platform Support
- Linux (x86_64, ARM64)
- macOS (x86_64, ARM64)
- Windows (x86_64)

[Unreleased]: https://github.com/intellekthq/duckdb-pst/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/intellekthq/duckdb-pst/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/intellekthq/duckdb-pst/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/intellekthq/duckdb-pst/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/intellekthq/duckdb-pst/releases/tag/v0.0.1

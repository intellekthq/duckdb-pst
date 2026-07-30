# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Bumped `microsoft-pst-sdk` to `bc1732c`, fixing non-ASCII text on macOS outside a UTF-8 locale. `bytes_to_wstring` asked iconv for `WCHAR_T`, which macOS resolves through `mbrtowc` and so follows `LC_CTYPE`; with no `LANG` set, as in CI, launchd or a container, anything above U+007F failed to decode. A normal terminal session was unaffected
- Added in-place deletion: `delete_pst_messages`, `delete_pst_folders`, `delete_pst_attachments` and `wipe_pst_free_space`
  - Gated behind `SET pst_allow_delete = true` and a per-call `really := true`; without `really` the call previews
  - Targets come from a nested read; the input table is positional and checked at bind time
  - Node IDs are checked against the mode, so `delete_pst_messages` refuses a folder or the store's root
  - Bind errors abort the statement, everything later is reported per row as `FAILED`
  - Paths carrying a scheme other than `file://` are refused
- Added `node_id` attachment struct field, the subnode ID taken by `delete_pst_attachments`
- Fixed `recipients` and `attachments` returning `NULL` instead of an empty list for messages that have none
- Registered `PSTFilterOptimizer` in `LoadInternal` rather than `Extension::Load`; the loadable build enters through `DUCKDB_CPP_EXTENSION_ENTRY` and never calls `Load`, so `LOAD pst` had no filter optimizer

## [0.1.3] - 2026-04-12

- Bumped DuckDB submodule to 1.5.1

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

[Unreleased]: https://github.com/intellekthq/duckdb-pst/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/intellekthq/duckdb-pst/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/intellekthq/duckdb-pst/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/intellekthq/duckdb-pst/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/intellekthq/duckdb-pst/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/intellekthq/duckdb-pst/releases/tag/v0.0.1

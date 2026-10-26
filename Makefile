PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=pst
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckDB 2.0 requires an explicit release identity for loadable extensions.
export DUCKDB_VERSION ?= $(shell git -C "$(PROJ_DIR)duckdb" describe --tags --exact-match --match 'v[0-9]*' 2>/dev/null)

# properly (?) set c++17
# Override VCPKG_OVERLAY_TRIPLETS to use dynamic CRT (/MD) on Windows,
# matching DuckDB's loadable extension CRT linkage.
EXT_FLAGS=-DCMAKE_CXX_STANDARD=17

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Formatting targets
clang-format:
	find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i

clang-format-check:
	find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format --dry-run --Werror

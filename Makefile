PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=pst
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Use an exact release tag or the actual source commit, never a nearby release.
# CI may supply a staged prebuilt identity for that same commit.
export DUCKDB_VERSION ?= $(shell git -C "$(PROJ_DIR)duckdb" describe --tags --exact-match --match 'v[0-9]*' 2>/dev/null || git -C "$(PROJ_DIR)duckdb" rev-parse HEAD)

# properly (?) set c++17
# Override VCPKG_OVERLAY_TRIPLETS to use dynamic CRT (/MD) on Windows,
# matching DuckDB's loadable extension CRT linkage.
EXT_FLAGS=-DCMAKE_CXX_STANDARD=17

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Keep SQL test globs intact: Catch treats expanded filenames as an intersection.
define RUN_TEST
	@if [ "$(SKIP_TESTS)" = "1" ]; then \
		echo "Tests are skipped in this run..."; \
	else \
		set -f; \
		$(TEST_RUNNER) ./build/$1/$(TEST_PATH) $(T); \
	fi
endef

# Formatting targets
clang-format:
	find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i

clang-format-check:
	find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format --dry-run --Werror

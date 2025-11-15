PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=pst
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Formatting targets
clang-format:
	find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i

clang-format-check:
	find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format --dry-run --Werror
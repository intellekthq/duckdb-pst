# This file is included by DuckDB's build system. It specifies which extension to load

set(DUCKPST_PATCHES_DIR "${CMAKE_SOURCE_DIR}/../.github/patches")

# Extension from this repo
duckdb_extension_load(pst
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# For convenience during testing
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG main
        APPLY_PATCHES
    )

#    duckdb_extension_load(aws
#        GIT_URL https://github.com/duckdb/duckdb-aws
#        GIT_TAG v1.4-andium
#    )

    # TODO: https://github.com/duckdb/duckdb/pull/19369
    # also, this runs twice??
    if(NOT EXISTS ${CMAKE_SOURCE_DIR}/.github/patches/extensions/ui)
        execute_process(
            COMMAND ln -s ${DUCKPST_PATCHES_DIR}/extensions/ui ${CMAKE_SOURCE_DIR}/.github/patches/extensions/ui
        )
    endif()

    duckdb_extension_load(ui
        GIT_URL https://github.com/duckdb/duckdb-ui
        GIT_TAG main
        APPLY_PATCHES
    )
endif()

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
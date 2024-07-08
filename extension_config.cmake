# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(duckdb_bigquery
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LINKED_LIBS "${VCPKG_INSTALLED_DIR}/wasm32-emscripten/lib/*_static.a"
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
#pragma once

#include "function_state.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "pstsdk/ltp/object.h"
#include "pstsdk/util/primitives.h"

// Everything that we want to emit as a row or column
namespace intellekt::duckpst::row_serializer {

template <typename T>
duckdb::Value from_prop(const LogicalType &t, pstsdk::const_property_object &bag, pstsdk::prop_id prop);

template <typename Item>
void set_output_column(PSTIteratorLocalTableFunctionState &local_state, duckdb::DataChunk &output, Item &item,
                       idx_t row_number, idx_t column_index);

template <typename Item>
void into_row(PSTIteratorLocalTableFunctionState &local_state, duckdb::DataChunk &output, Item &item, idx_t row_number);

} // namespace intellekt::duckpst::row_serializer

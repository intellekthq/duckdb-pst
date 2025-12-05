#pragma once

#include "function_state.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "pstsdk/ltp/object.h"
#include "pstsdk/util/primitives.h"

namespace intellekt::duckpst::row_serializer {

template <typename T>
duckdb::Value from_prop(const LogicalType &t, pstsdk::const_property_object &bag, pstsdk::prop_id prop);

template <typename Item>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output, Item &item, idx_t row_number,
                       idx_t column_index);

template <typename Item>
void into_row(PSTReadLocalState &local_state, duckdb::DataChunk &output, Item &item, idx_t row_number);

template <typename Item>
duckdb::Value into_struct(const LogicalType &t, Item item);

void set_common_struct_fields(vector<Value> &values, pstsdk::const_property_object &bag);

} // namespace intellekt::duckpst::row_serializer

#pragma once

#include "function_state.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "pstsdk/ltp/object.h"
#include "pstsdk/util/primitives.h"

/**
 * @brief Obtain duckdb::Value from pstsdk objects
 *
 */
namespace intellekt::duckpst::row_serializer {

/**
 * @brief Given a prop ID (against its CXX runtime type), make a DuckDB value.
 *
 * @tparam T e.g., idx_t, or std::string, or std::wstring
 * @param t The logical type of the target column
 * @param bag A pstsdk prop bag
 * @param prop A MAPI property ID
 * @return duckdb::Value
 */
template <typename T>
duckdb::Value from_prop(const LogicalType &t, pstsdk::const_property_object &bag, pstsdk::prop_id prop);

/**
 * @brief Same as from_prop, but against a stream reader with a specified read size.
 *
 * @tparam T e.g., idx_t, or std::string, or std::wstring
 * @param t The logical type of the target column
 * @param bag A pstsdk prop bag
 * @param prop A MAPI property ID
 * @param read_size_bytes How many bytes to read
 * @return duckdb::Value
 */
template <typename T>
duckdb::Value from_prop_stream(const LogicalType &t, pstsdk::const_property_object &bag, pstsdk::prop_id prop,
                               idx_t read_size_bytes);

/**
 * @brief Set an output column for the current row
 *
 * @tparam Item A pstsdk type (either message, or folder, or const_property_object)
 * @param local_state Local read state
 * @param output Target data chunk
 * @param item pstsdk object being read
 * @param row_number Row number
 * @param column_index Column index (use column_ids to resolve against schema)
 */
template <typename Item>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output, Item &item, idx_t row_number,
                       idx_t column_index);

/**
 * @brief Append a row to the output chunk
 *
 * @tparam Item A pstsdk type (either message, or folder, or const_property_object)
 * @param local_state Local read state
 * @param output Target data chunk
 * @param item pstsdk object being read
 * @param row_number Row number
 */
template <typename Item>
void into_row(PSTReadLocalState &local_state, duckdb::DataChunk &output, Item &item, idx_t row_number);

/**
 * @brief Make a struct value from a pstsdk item
 *
 * @tparam Item A pstsdk type (either message, or folder, or const_property_object)
 * @param local_state Local read state
 * @param t Struct schema
 * @param item pstsdk object being read
 * @return duckdb::Value
 */
template <typename Item>
duckdb::Value into_struct(PSTReadLocalState &local_state, const LogicalType &t, Item item);

/**
 * @brief Set common MAPI property values in a struct's child list
 *
 * @param values
 * @param bag
 */
void set_common_struct_fields(vector<Value> &values, pstsdk::const_property_object &bag);

} // namespace intellekt::duckpst::row_serializer

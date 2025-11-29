#include "row_serializer.hpp"
#include "schema.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/logger.hpp"
#include "pstsdk/ltp/object.h"
#include "pstsdk/ltp/propbag.h"
#include "pstsdk/mapitags.h"
#include "pstsdk/pst/folder.h"
#include "pstsdk/util/primitives.h"
#include <cstdint>
#include <type_traits>

namespace intellekt::duckpst::row_serializer {

template <typename T>
duckdb::Value from_prop(const LogicalType &t, pstsdk::const_property_object &bag, pstsdk::prop_id prop) {
	if (!bag.prop_exists(prop))
		return Value(nullptr);

	std::optional<T> value = bag.read_prop_if_exists<T>(prop);
	Value duckdb_value = Value(nullptr);

	if (!value.has_value())
		return duckdb_value;

	if constexpr (std::is_integral_v<T>) {
		// TODO
		if (t.id() == LogicalTypeId::ENUM) {
			auto enum_size = EnumType::GetSize(t);
			if (*value >= enum_size)
				return duckdb_value;
			duckdb_value = Value::ENUM(*value, t);
		} else if (t.id() == LogicalTypeId::TIMESTAMP_SEC) {
			time_t unixtime = pstsdk::filetime_to_time_t(*value);
			duckdb_value = Value::TIMESTAMPSEC(timestamp_sec_t(unixtime));
		}
	} else if constexpr (std::is_same_v<T, std::vector<unsigned char>>) {
		std::string blob({value->begin(), value->end()});
		duckdb_value = Value::BLOB_RAW(blob);
	}

	if (!duckdb_value.IsNull())
		return duckdb_value;

	if constexpr (std::is_same_v<T, std::uint8_t>) {
		duckdb_value = Value::UTINYINT(*value);
	} else if constexpr (std::is_same_v<T, std::uint16_t>) {
		duckdb_value = Value::USMALLINT(*value);
	} else if constexpr (std::is_same_v<T, std::uint32_t>) {
		duckdb_value = Value::UINTEGER(*value);
	} else if constexpr (std::is_same_v<T, std::uint64_t>) {
		duckdb_value = Value::UBIGINT(*value);
	} else if constexpr (std::is_same_v<T, unsigned long long>) {
		duckdb_value = Value::UHUGEINT(*value);
	} else if constexpr (std::is_same_v<T, std::int8_t>) {
		duckdb_value = Value::TINYINT(*value);
	} else if constexpr (std::is_same_v<T, std::int16_t>) {
		duckdb_value = Value::SMALLINT(*value);
	} else if constexpr (std::is_same_v<T, std::int32_t>) {
		duckdb_value = Value::INTEGER(*value);
	} else if constexpr (std::is_same_v<T, std::int64_t>) {
		duckdb_value = Value::BIGINT(*value);
	} else if constexpr (std::is_same_v<T, long long>) {
		duckdb_value = Value::HUGEINT(*value);
	} else if constexpr (!std::is_same_v<T, std::vector<unsigned char>>) {
		duckdb_value = Value(*value);
	}

	return duckdb_value;
}

template <>
void set_output_column(PSTIteratorLocalTableFunctionState &local_state, duckdb::DataChunk &output, pstsdk::message &msg,
                       idx_t row_number, idx_t column_index) {
	auto &pst_prop_bag = const_cast<pstsdk::property_bag &>(local_state.current_pst()->get_property_bag());
	auto &prop_bag = msg.get_property_bag();
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.global_state.output_schema, schema_col);
	switch (schema_col) {
	case static_cast<int>(schema::MessageProjection::pst_path):
		output.SetValue(column_index, row_number, Value(local_state.current_file().path));
		break;
	case static_cast<int>(schema::MessageProjection::pst_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, pst_prop_bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::MessageProjection::folder_id):
		output.SetValue(column_index, row_number, Value::UINTEGER(prop_bag.get_node().get_parent_id()));
		break;
	case static_cast<int>(schema::MessageProjection::message_id):
		output.SetValue(column_index, row_number, Value::UINTEGER(msg.get_id()));
		break;
	case static_cast<int>(schema::MessageProjection::subject):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_SUBJECT_A));
		break;
	case static_cast<int>(schema::MessageProjection::sender_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_SENDER_NAME_A));
		break;
	case static_cast<int>(schema::MessageProjection::sender_email_address):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_SENDER_EMAIL_ADDRESS_A));
		break;
	case static_cast<int>(schema::MessageProjection::message_delivery_time):
		output.SetValue(column_index, row_number,
		                from_prop<pstsdk::ulonglong>(col_type, prop_bag, PR_MESSAGE_DELIVERY_TIME));
		break;
	case static_cast<int>(schema::MessageProjection::message_class):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_MESSAGE_CLASS_A));
		break;
	case static_cast<int>(schema::MessageProjection::importance):
		output.SetValue(column_index, row_number, from_prop<uint32_t>(col_type, prop_bag, PR_IMPORTANCE));
		break;
	case static_cast<int>(schema::MessageProjection::sensitivity):
		output.SetValue(column_index, row_number, from_prop<uint32_t>(col_type, prop_bag, PR_SENSITIVITY));
		break;
	case static_cast<int>(schema::MessageProjection::message_flags):
		output.SetValue(column_index, row_number, from_prop<uint32_t>(col_type, prop_bag, PR_MESSAGE_FLAGS));
		break;
	case static_cast<int>(schema::MessageProjection::message_size):
		output.SetValue(column_index, row_number, Value::BIGINT(msg.size()));
		break;
	case static_cast<int>(schema::MessageProjection::has_attachments): {
		size_t attachment_count = msg.get_attachment_count();
		output.SetValue(column_index, row_number, Value::BOOLEAN(attachment_count > 0));
		break;
	}
	case static_cast<int>(schema::MessageProjection::attachment_count): {
		size_t attachment_count = msg.get_attachment_count();
		output.SetValue(column_index, row_number, Value::UINTEGER(attachment_count));
		break;
	}
	case static_cast<int>(schema::MessageProjection::body):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_BODY_A));
		break;
	case static_cast<int>(schema::MessageProjection::body_html):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_HTML));
		break;
	case static_cast<int>(schema::MessageProjection::internet_message_id):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_INTERNET_MESSAGE_ID));
		break;
	case static_cast<int>(schema::MessageProjection::conversation_topic):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_CONVERSATION_TOPIC_A));
		break;
	case static_cast<int>(schema::MessageProjection::recipients): {
		vector<Value> recipients;
		for (auto it = msg.recipient_begin(); it != msg.recipient_end(); ++it) {
			auto recipient = *it;
			auto recipient_prop_bag = recipient.get_property_row();

			vector<Value> values;

			values.emplace_back(from_prop<std::string>(StructType::GetChildType(schema::RECIPIENT_SCHEMA, 0),
			                                           recipient_prop_bag, PR_DISPLAY_NAME_A));
			values.emplace_back(from_prop<std::string>(StructType::GetChildType(schema::RECIPIENT_SCHEMA, 1),
			                                           recipient_prop_bag, PR_ACCOUNT_A));
			values.emplace_back(from_prop<std::string>(StructType::GetChildType(schema::RECIPIENT_SCHEMA, 2),
			                                           recipient_prop_bag, PR_EMAIL_ADDRESS_A));
			values.emplace_back(from_prop<std::string>(StructType::GetChildType(schema::RECIPIENT_SCHEMA, 3),
			                                           recipient_prop_bag, PR_ADDRTYPE_A));
			values.emplace_back(from_prop<int32_t>(StructType::GetChildType(schema::RECIPIENT_SCHEMA, 4),
			                                       recipient_prop_bag, PR_RECIPIENT_TYPE));
			values.emplace_back(from_prop<int32_t>(StructType::GetChildType(schema::RECIPIENT_SCHEMA, 5),
			                                       recipient_prop_bag, PR_RECIPIENT_TYPE));

			recipients.emplace_back(Value::STRUCT(schema::RECIPIENT_SCHEMA, values));
		}
		output.SetValue(column_index, row_number, Value::LIST(schema::RECIPIENT_SCHEMA, recipients));
		break;
	}
	case static_cast<int>(schema::MessageProjection::attachments): {
		vector<Value> attachments;
		for (auto it = msg.attachment_begin(); it != msg.attachment_end(); ++it) {
			auto attachment = *it;
			auto attachment_prop_bag = attachment.get_property_bag();

			vector<Value> values;
			values.emplace_back(from_prop<int32_t>(StructType::GetChildType(schema::ATTACHMENT_SCHEMA, 0),
			                                       attachment_prop_bag, PR_ATTACH_NUM));
			values.emplace_back(from_prop<int32_t>(StructType::GetChildType(schema::ATTACHMENT_SCHEMA, 1),
			                                       attachment_prop_bag, PR_ATTACH_METHOD));
			values.emplace_back(from_prop<std::string>(StructType::GetChildType(schema::ATTACHMENT_SCHEMA, 2),
			                                           attachment_prop_bag, PR_ATTACH_FILENAME_A));
			values.emplace_back(from_prop<std::string>(StructType::GetChildType(schema::ATTACHMENT_SCHEMA, 3),
			                                           attachment_prop_bag, PR_ATTACH_MIME_TAG_A));
			values.emplace_back(from_prop<uint32_t>(StructType::GetChildType(schema::ATTACHMENT_SCHEMA, 4),
			                                        attachment_prop_bag, PR_ATTACH_SIZE));

			// when ATTACH_BY_VALUE
			// TODO: support the other attach methods
			// https://stackoverflow.com/a/4693174
			values.emplace_back(Value(attachment.is_message()));
			values.emplace_back(from_prop<std::vector<pstsdk::byte>>(
			    StructType::GetChildType(schema::ATTACHMENT_SCHEMA, 6), attachment_prop_bag, PR_ATTACH_DATA_BIN));

			attachments.emplace_back(Value::STRUCT(schema::ATTACHMENT_SCHEMA, values));
		}

		output.SetValue(column_index, row_number, Value::LIST(schema::ATTACHMENT_SCHEMA, attachments));
		break;
	}
	default:
		break;
	}
}

template <>
void set_output_column(PSTIteratorLocalTableFunctionState &local_state, duckdb::DataChunk &output,
                       pstsdk::folder &folder, idx_t row_number, idx_t column_index) {
	auto &prop_bag = folder.get_property_bag();
	auto &pst_prop_bag = const_cast<pstsdk::property_bag &>(local_state.current_pst()->get_property_bag());
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.global_state.output_schema, schema_col);

	switch (schema_col) {
	case static_cast<int>(schema::FolderProjection::pst_path):
		output.SetValue(schema_col, row_number, Value(local_state.current_file().path));
		break;
	case static_cast<int>(schema::FolderProjection::pst_name):
		output.SetValue(schema_col, row_number, from_prop<std::string>(col_type, pst_prop_bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::FolderProjection::parent_folder_id):
		output.SetValue(schema_col, row_number, Value::UINTEGER(prop_bag.get_node().get_parent_id()));
		break;
	case static_cast<int>(schema::FolderProjection::folder_id):
		output.SetValue(schema_col, row_number, Value::UINTEGER(folder.get_id()));
		break;
	case static_cast<int>(schema::FolderProjection::folder_name):
		output.SetValue(schema_col, row_number, from_prop<std::string>(col_type, prop_bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::FolderProjection::subfolder_count):
		output.SetValue(schema_col, row_number, Value::UINTEGER(folder.get_subfolder_count()));
		break;
	case static_cast<int>(schema::FolderProjection::message_count):
		output.SetValue(schema_col, row_number, Value::BIGINT(folder.get_message_count()));
		break;
	case static_cast<int>(schema::FolderProjection::unread_message_count):
		output.SetValue(schema_col, row_number, Value::BIGINT(folder.get_unread_message_count()));
		break;
	default:
		break;
	}
}

template <typename Item>
void into_row(PSTIteratorLocalTableFunctionState &local_state, DataChunk &output, Item &item, idx_t row_number) {
	for (idx_t col_idx = 0; col_idx < local_state.column_ids().size(); ++col_idx) {
		try {
			set_output_column<Item>(local_state, output, item, row_number, col_idx);
		} catch (std::exception &e) {
			auto schema_col = local_state.column_ids()[col_idx];
			auto &output_schema = local_state.global_state.output_schema;

			DUCKDB_LOG_ERROR(local_state.ec, "Failed to read column: %s (%s)\nError: %s",
			                 StructType::GetChildName(output_schema, schema_col),
			                 StructType::GetChildType(output_schema, schema_col).ToString(), e.what());
		}
	}
}

template void into_row<pstsdk::folder>(PSTIteratorLocalTableFunctionState &, DataChunk &, pstsdk::folder &, idx_t);
template void into_row<pstsdk::message>(PSTIteratorLocalTableFunctionState &, DataChunk &, pstsdk::message &, idx_t);

} // namespace intellekt::duckpst::row_serializer

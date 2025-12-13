#include "function_state.hpp"
#include "row_serializer.hpp"
#include "schema.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/logger.hpp"
#include "pstsdk/ltp/object.h"
#include "pstsdk/ltp/propbag.h"
#include "pstsdk/mapitags.h"
#include "pstsdk/pst/folder.h"
#include "pstsdk/pst/message.h"
#include "pstsdk/util/primitives.h"
#include "pstsdk/util/util.h"

#include <cstdint>
#include <type_traits>

namespace intellekt::duckpst::row_serializer {

template <typename T>
duckdb::Value from_prop(const LogicalType &t, pstsdk::const_property_object &bag, pstsdk::prop_id prop) {
	std::optional<T> value = bag.read_prop_if_exists<T>(prop);
	Value duckdb_value = Value(nullptr);

	if (!value.has_value())
		return duckdb_value;

	if constexpr (std::is_integral_v<T>) {
		if (t.id() == LogicalTypeId::ENUM) {
			auto as_u64 = boost::numeric_cast<uint64_t>(*value);
			if (as_u64 >= EnumType::GetSize(t))
				return duckdb_value;

			duckdb_value = Value::ENUM(boost::numeric_cast<uint64_t>(*value), t);
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

template <typename T>
duckdb::Value from_prop_stream(const LogicalType &t, pstsdk::const_property_object &bag, pstsdk::prop_id prop,
                               idx_t read_size_bytes) {
	Value duckdb_value = Value(nullptr);
	if (!bag.prop_exists(prop))
		return duckdb_value;

	auto prop_type = bag.get_prop_type(prop);
	auto stream = bag.open_prop_stream(prop);

	// TODO: shitty force align for wchar, have to do it because some PST writers lie about the type
	if ((read_size_bytes % 2) != 0)
		++read_size_bytes;

	vector<pstsdk::byte> buf(read_size_bytes);
	stream.read(reinterpret_cast<char *>(buf.data()), read_size_bytes);
	stream.close();

	if constexpr (std::is_same_v<T, std::string>) {
		if (prop_type == pstsdk::prop_type_string) {
			duckdb_value = Value(std::string(buf.begin(), buf.end()));
		} else {
			duckdb_value = Value(std::string(pstsdk::bytes_to_string(buf)));
		}
	} else if constexpr (std::is_same_v<T, vector<pstsdk::byte>>) {
		duckdb_value = Value::BLOB_RAW(std::string(buf.begin(), buf.end()));
	}

	return duckdb_value;
}

template <>
duckdb::Value into_struct(PSTReadLocalState &local_state, const LogicalType &t, pstsdk::attachment attachment) {
	auto attachment_prop_bag = attachment.get_property_bag();
	vector<Value> values(StructType::GetChildCount(t), Value(nullptr));

	for (idx_t col = 0; col < values.size(); ++col) {
		auto &col_type = StructType::GetChildType(t, col);

		switch (col) {
		case static_cast<int>(schema::AttachmentProjection::attach_content_id):
			values[col] = from_prop<std::string>(col_type, attachment_prop_bag, PR_ATTACH_CONTENT_ID);
			break;
		case static_cast<int>(schema::AttachmentProjection::attach_method):
			values[col] = from_prop<int32_t>(col_type, attachment_prop_bag, PR_ATTACH_METHOD);
			break;
		case static_cast<int>(schema::AttachmentProjection::filename):
			values[col] = from_prop<std::string>(col_type, attachment_prop_bag, PR_ATTACH_FILENAME_A);
			break;
		case static_cast<int>(schema::AttachmentProjection::mime_type):
			values[col] = from_prop<std::string>(col_type, attachment_prop_bag, PR_ATTACH_MIME_TAG_A);
			break;
		case static_cast<int>(schema::AttachmentProjection::size):
			if (!attachment_prop_bag.prop_exists(PR_ATTACH_DATA_BIN))
				break;
			values[col] = Value::UBIGINT(attachment.content_size());
			break;
		case static_cast<int>(schema::AttachmentProjection::is_message):
			if (!attachment_prop_bag.prop_exists(PR_ATTACH_METHOD))
				break;
			values[col] = Value::BOOLEAN(attachment.is_message());
			break;
		case static_cast<int>(schema::AttachmentProjection::bytes):
			if (!attachment_prop_bag.prop_exists(PR_ATTACH_METHOD) ||
			    !attachment_prop_bag.prop_exists(PR_ATTACH_DATA_BIN) || attachment.is_message() ||
			    attachment.content_size() <= 0 || !local_state.global_state.bind_data.read_attachment_body()) {
				break;
			}
			values[col] = from_prop<std::vector<pstsdk::byte>>(col_type, attachment_prop_bag, PR_ATTACH_DATA_BIN);
			break;
		default:
			set_common_struct_fields(values, attachment_prop_bag, col_type, col);
		}
	}

	return Value::STRUCT(t, values);
}

template <>
duckdb::Value into_struct(PSTReadLocalState &local_state, const LogicalType &t, pstsdk::recipient recipient) {
	auto recipient_prop_bag = recipient.get_property_row();
	vector<Value> values(StructType::GetChildCount(t), Value(nullptr));

	for (idx_t col = 0; col < values.size(); ++col) {
		auto &col_type = StructType::GetChildType(t, col);

		switch (col) {
		case static_cast<int>(schema::RecipientProjection::account_name):
			values[col] = from_prop<std::string>(col_type, recipient_prop_bag, PR_ACCOUNT_A);
			break;
		case static_cast<int>(schema::RecipientProjection::email_address):
			values[col] = from_prop<std::string>(col_type, recipient_prop_bag, PR_EMAIL_ADDRESS_A);
			break;
		case static_cast<int>(schema::RecipientProjection::address_type):
			values[col] = from_prop<std::string>(col_type, recipient_prop_bag, PR_ADDRTYPE_A);
			break;
		case static_cast<int>(schema::RecipientProjection::recipient_type):
			values[col] = from_prop<int32_t>(col_type, recipient_prop_bag, PR_RECIPIENT_TYPE);
			break;
		case static_cast<int>(schema::RecipientProjection::recipient_type_raw):
			values[col] = from_prop<int32_t>(col_type, recipient_prop_bag, PR_RECIPIENT_TYPE);
			break;
		default:
			set_common_struct_fields(values, recipient_prop_bag, col_type, col);
		}
	}
	return Value::STRUCT(t, values);
}

void set_common_struct_fields(vector<Value> &values, pstsdk::const_property_object &bag, const LogicalType &col_type,
                              idx_t col) {
	switch (col) {
	case static_cast<int>(schema::CommonProjection::display_name):
		values[col] = from_prop<std::string>(col_type, bag, PR_DISPLAY_NAME_A);
		break;
	case static_cast<int>(schema::CommonProjection::comment):
		values[col] = from_prop<std::string>(col_type, bag, PR_COMMENT_A);
		break;
	case static_cast<int>(schema::CommonProjection::creation_time):
		values[col] = from_prop<pstsdk::ulonglong>(col_type, bag, PR_CREATION_TIME);
		break;
	case static_cast<int>(schema::CommonProjection::last_modified):
		values[col] = from_prop<pstsdk::ulonglong>(col_type, bag, PR_LAST_MODIFICATION_TIME);
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output, pstsdk::const_property_object &bag,
                       idx_t row_number, idx_t column_index) {
	auto schema_col = local_state.column_ids()[column_index];
	auto &pst_bag = local_state.pst->get_property_bag();
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	switch (schema_col) {
	case static_cast<int>(schema::PSTCommonChildren::pst_path):
		output.SetValue(column_index, row_number, Value(local_state.partition->file.path));
		break;
	case static_cast<int>(schema::PSTCommonChildren::pst_name):
		output.SetValue(
		    column_index, row_number,
		    from_prop<std::string>(col_type, const_cast<pstsdk::property_bag &>(pst_bag), PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::PSTCommonChildren::record_key):
		output.SetValue(
		    column_index, row_number,
		    from_prop<std::vector<pstsdk::byte>>(col_type, const_cast<pstsdk::property_bag &>(pst_bag), PR_RECORD_KEY));
		break;
	case static_cast<int>(schema::PSTCommonChildren::display_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::PSTCommonChildren::comment):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, bag, PR_COMMENT_A));
		break;
	case static_cast<int>(schema::PSTCommonChildren::creation_time):
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, bag, PR_CREATION_TIME));
		break;
	case static_cast<int>(schema::PSTCommonChildren::last_modified):
		output.SetValue(column_index, row_number,
		                from_prop<pstsdk::ulonglong>(col_type, bag, PR_LAST_MODIFICATION_TIME));
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output, pstsdk::pst &pst, idx_t row_number,
                       idx_t column_index) {
	auto schema_col = local_state.column_ids()[column_index];
	auto &pst_bag = pst.get_property_bag();
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	switch (schema_col) {
	case static_cast<int>(schema::PSTProjection::pst_path):
		output.SetValue(column_index, row_number, Value(local_state.partition->file.path));
		break;
	case static_cast<int>(schema::PSTProjection::pst_name):
		output.SetValue(
		    column_index, row_number,
		    from_prop<std::string>(col_type, const_cast<pstsdk::property_bag &>(pst_bag), PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::PSTProjection::record_key):
		output.SetValue(
		    column_index, row_number,
		    from_prop<std::vector<pstsdk::byte>>(col_type, const_cast<pstsdk::property_bag &>(pst_bag), PR_RECORD_KEY));
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output, pstsdk::message &msg,
                       idx_t row_number, idx_t column_index) {
	auto &prop_bag = msg.get_property_bag();
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);
	auto read_size = local_state.global_state.bind_data.max_body_size_bytes();

	switch (schema_col) {
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
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, PR_IMPORTANCE));
		break;
	case static_cast<int>(schema::MessageProjection::sensitivity):
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, PR_SENSITIVITY));
		break;
	case static_cast<int>(schema::MessageProjection::message_flags):
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, PR_MESSAGE_FLAGS));
		break;
	case static_cast<int>(schema::MessageProjection::message_size):
		output.SetValue(column_index, row_number, Value::UBIGINT(msg.size()));
		break;
	case static_cast<int>(schema::MessageProjection::has_attachments): {
		size_t attachment_count = msg.get_attachment_count();
		output.SetValue(column_index, row_number, Value::BOOLEAN(attachment_count > 0));
		break;
	}
	case static_cast<int>(schema::MessageProjection::attachment_count): {
		size_t attachment_count = msg.get_attachment_count();
		output.SetValue(column_index, row_number, Value::UBIGINT(attachment_count));
		break;
	}
	case static_cast<int>(schema::MessageProjection::body):
		if (!prop_bag.prop_exists(PR_BODY_A)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			return;
		}

		if (read_size == 0)
			read_size = msg.body_size();
		read_size = std::min<idx_t>(read_size, msg.body_size());
		output.SetValue(column_index, row_number,
		                from_prop_stream<std::string>(col_type, prop_bag, PR_BODY_A, read_size));
		break;
	case static_cast<int>(schema::MessageProjection::body_html):
		if (!prop_bag.prop_exists(PR_HTML)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			return;
		}

		if (read_size == 0)
			read_size = msg.html_body_size();
		read_size = std::min<idx_t>(read_size, msg.html_body_size());
		output.SetValue(column_index, row_number,
		                from_prop_stream<std::string>(col_type, prop_bag, PR_HTML, read_size));
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
			try {
				recipients.emplace_back(into_struct(local_state, schema::RECIPIENT_SCHEMA, *it));
			} catch (std::exception &e) {
				DUCKDB_LOG_ERROR(local_state.ec, "Unable to serialize recipient struct: %s", e.what());
				recipients.emplace_back(Value(nullptr));
			}
		}
		output.SetValue(column_index, row_number, Value::LIST(schema::RECIPIENT_SCHEMA, recipients));
		break;
	}
	case static_cast<int>(schema::MessageProjection::attachments): {
		vector<Value> attachments;
		for (auto it = msg.attachment_begin(); it != msg.attachment_end(); ++it) {
			try {
				attachments.emplace_back(into_struct(local_state, schema::ATTACHMENT_SCHEMA, *it));
			} catch (std::exception &e) {
				DUCKDB_LOG_ERROR(local_state.ec, "Unable to serialize attachment struct: %s", e.what());
				attachments.emplace_back(Value(nullptr));
			}
		}

		output.SetValue(column_index, row_number, Value::LIST(schema::ATTACHMENT_SCHEMA, attachments));
		break;
	}
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output, pstsdk::folder &folder,
                       idx_t row_number, idx_t column_index) {
	auto &prop_bag = folder.get_property_bag();
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	switch (schema_col) {
	case static_cast<int>(schema::FolderProjection::display_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::FolderProjection::parent_node_id):
		output.SetValue(column_index, row_number, Value::UINTEGER(prop_bag.get_node().get_parent_id()));
		break;
	case static_cast<int>(schema::FolderProjection::subfolder_count):
		output.SetValue(column_index, row_number, Value::UINTEGER(folder.get_subfolder_count()));
		break;
	case static_cast<int>(schema::FolderProjection::message_count):
		output.SetValue(column_index, row_number, Value::BIGINT(folder.get_message_count()));
		break;
	case static_cast<int>(schema::FolderProjection::unread_message_count):
		output.SetValue(column_index, row_number, Value::BIGINT(folder.get_unread_message_count()));
		break;
	default:
		break;
	}
}

template <typename Item>
void into_row(PSTReadLocalState &local_state, DataChunk &output, Item &item, idx_t row_number) {
	for (idx_t col_idx = 0; col_idx < local_state.column_ids().size(); ++col_idx) {
		auto schema_col = local_state.column_ids()[col_idx];

		// Bind virtual columns + node_id (should be 'infallible' as long as the file isn't borked)
		switch (schema_col) {
		case schema::PST_ITEM_NODE_ID:
			output.SetValue(col_idx, row_number, Value::UINTEGER(item.get_id()));
			break;
		case schema::PST_PARTITION_INDEX:
			output.SetValue(col_idx, row_number, Value::UBIGINT(local_state.partition->partition_index));
			break;
		case static_cast<int>(schema::PSTProjection::node_id):
			output.SetValue(col_idx, row_number, Value::UINTEGER(item.get_id()));
			break;
		default:
			try {
				// Bind PST attributes
				set_output_column<pstsdk::pst>(local_state, output, *local_state.pst, row_number, col_idx);

				// Bind common columns (message only)
				if constexpr (std::is_same_v<Item, pstsdk::message>) {
					set_output_column<const_property_object>(local_state, output,
					                                         static_cast<pstsdk::message>(item).get_property_bag(),
					                                         row_number, col_idx);
				}

				set_output_column<Item>(local_state, output, item, row_number, col_idx);
			} catch (std::exception &e) {
				auto schema_col = local_state.column_ids()[col_idx];
				auto &output_schema = local_state.output_schema();

				DUCKDB_LOG_ERROR(local_state.ec, "Failed to read column: %s (%s)\nError: %s",
				                 StructType::GetChildName(output_schema, schema_col),
				                 StructType::GetChildType(output_schema, schema_col).ToString(), e.what());

				output.SetValue(col_idx, row_number, Value(nullptr));
			}
		}
	}
}

template void into_row<pstsdk::folder>(PSTReadLocalState &, DataChunk &, pstsdk::folder &, idx_t);
template void into_row<pstsdk::message>(PSTReadLocalState &, DataChunk &, pstsdk::message &, idx_t);

} // namespace intellekt::duckpst::row_serializer

#include "duckdb/common/exception.hpp"
#include "function_state.hpp"
#include "row_serializer.hpp"
#include "pstsdk/pst/entryid.h"
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
#include "pst/typed_bag.hpp"
#include "table_function.hpp"

#include <cstdint>
#include <cstring>
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
			break;
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
		case static_cast<int>(schema::RecipientProjection::display_name):
			values[col] = from_prop<std::string>(col_type, recipient_prop_bag, PR_DISPLAY_NAME_A);
			break;
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
			break;
		}
	}
	return Value::STRUCT(t, values);
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
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, pst_bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::PSTProjection::record_key):
		output.SetValue(column_index, row_number,
		                from_prop<std::vector<pstsdk::byte>>(col_type, pst_bag, PR_RECORD_KEY));
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
	auto read_size = local_state.global_state.bind_data.read_body_size_bytes();

	switch (schema_col) {
	case static_cast<int>(schema::NoteProjection::display_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::NoteProjection::comment):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_COMMENT_A));
		break;
	case static_cast<int>(schema::NoteProjection::creation_time):
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, PR_CREATION_TIME));
		break;
	case static_cast<int>(schema::NoteProjection::last_modified):
		output.SetValue(column_index, row_number,
		                from_prop<pstsdk::ulonglong>(col_type, prop_bag, PR_LAST_MODIFICATION_TIME));
		break;
	case static_cast<int>(schema::NoteProjection::importance):
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, PR_IMPORTANCE));
		break;
	case static_cast<int>(schema::NoteProjection::priority): {
		// This can be -1, 0, 1, so we have to do a little extra work
		auto priority = prop_bag.read_prop_if_exists<int32_t>(PR_PRIORITY);
		if (priority) {
			auto enum_idx = *priority + 1;
			if (enum_idx < EnumType::GetSize(schema::PRIORITY_ENUM)) {
				output.SetValue(column_index, row_number, Value::ENUM(enum_idx, schema::PRIORITY_ENUM));
				return;
			}
		}
		output.SetValue(column_index, row_number, Value(nullptr));
		break;
	}
	case static_cast<int>(schema::NoteProjection::sensitivity):
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, PR_SENSITIVITY));
		break;
	case static_cast<int>(schema::NoteProjection::subject):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_SUBJECT_A));
		break;
	case static_cast<int>(schema::NoteProjection::body):
		if (!prop_bag.prop_exists(PR_BODY_A)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			return;
		}
		{
			auto body_size = prop_bag.size(PR_BODY_A);
			if (read_size == 0)
				read_size = prop_bag.size(PR_BODY_A);

			read_size = std::min<idx_t>(read_size, body_size);
			output.SetValue(column_index, row_number,
			                from_prop_stream<std::string>(col_type, prop_bag, PR_BODY_A, read_size));
		}
		break;
	case static_cast<int>(schema::NoteProjection::sender_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_SENDER_NAME_A));
		break;
	case static_cast<int>(schema::NoteProjection::sender_email_address):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_SENDER_EMAIL_ADDRESS_A));
		break;
	case static_cast<int>(schema::NoteProjection::message_delivery_time):
		output.SetValue(column_index, row_number,
		                from_prop<pstsdk::ulonglong>(col_type, prop_bag, PR_MESSAGE_DELIVERY_TIME));
		break;
	case static_cast<int>(schema::NoteProjection::message_class):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_MESSAGE_CLASS_A));
		break;
	case static_cast<int>(schema::NoteProjection::message_flags):
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, PR_MESSAGE_FLAGS));
		break;
	case static_cast<int>(schema::NoteProjection::message_size):
		output.SetValue(column_index, row_number, Value::UBIGINT(msg.size()));
		break;
	case static_cast<int>(schema::NoteProjection::has_attachments): {
		size_t attachment_count = msg.get_attachment_count();
		output.SetValue(column_index, row_number, Value::BOOLEAN(attachment_count > 0));
		break;
	}
	case static_cast<int>(schema::NoteProjection::attachment_count): {
		size_t attachment_count = msg.get_attachment_count();
		output.SetValue(column_index, row_number, Value::UBIGINT(attachment_count));
		break;
	}
	case static_cast<int>(schema::NoteProjection::body_html):
		if (!prop_bag.prop_exists(PR_HTML)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			return;
		}
		{
			auto body_size = prop_bag.size(PR_HTML);
			if (read_size == 0)
				read_size = body_size;
			read_size = std::min<idx_t>(read_size, body_size);
			output.SetValue(column_index, row_number,
			                from_prop_stream<std::string>(col_type, prop_bag, PR_HTML, read_size));
		}
		break;
	case static_cast<int>(schema::NoteProjection::internet_message_id):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_INTERNET_MESSAGE_ID));
		break;
	case static_cast<int>(schema::NoteProjection::conversation_topic):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_CONVERSATION_TOPIC_A));
		break;
	case static_cast<int>(schema::NoteProjection::recipients): {
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
	case static_cast<int>(schema::NoteProjection::attachments): {
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
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                       pst::TypedBag<pst::MessageClass::Contact> &contact, idx_t row_number, idx_t column_index) {
	auto &prop_bag = contact.bag;
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	switch (schema_col) {
	case static_cast<int>(schema::ContactProjection::account_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_ACCOUNT_A));
		break;
	case static_cast<int>(schema::ContactProjection::callback_number):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_CALLBACK_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::conversation_prohibited):
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, PR_CONVERSION_PROHIBITED));
		break;
	case static_cast<int>(schema::ContactProjection::disclose_recipients):
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, PR_DISCLOSE_RECIPIENTS));
		break;
	case static_cast<int>(schema::ContactProjection::generation_suffix):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_GENERATION_A));
		break;
	case static_cast<int>(schema::ContactProjection::given_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_GIVEN_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::government_id_number):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_GOVERNMENT_ID_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_BUSINESS_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_HOME_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::initials):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_INITIALS_A));
		break;
	case static_cast<int>(schema::ContactProjection::keyword):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_KEYWORD_A));
		break;
	case static_cast<int>(schema::ContactProjection::language):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_LANGUAGE_A));
		break;
	case static_cast<int>(schema::ContactProjection::location):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_LOCATION_A));
		break;
	case static_cast<int>(schema::ContactProjection::mail_permission):
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, PR_MAIL_PERMISSION));
		break;
	case static_cast<int>(schema::ContactProjection::mhs_common_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_MHS_COMMON_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::organizational_id_number):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_ORGANIZATIONAL_ID_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::surname):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_SURNAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::original_display_name):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_ORIGINAL_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::postal_address):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_POSTAL_ADDRESS_A));
		break;
	case static_cast<int>(schema::ContactProjection::company_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_COMPANY_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::title):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_TITLE_A));
		break;
	case static_cast<int>(schema::ContactProjection::department_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_DEPARTMENT_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::office_location):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_OFFICE_LOCATION_A));
		break;
	case static_cast<int>(schema::ContactProjection::primary_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_PRIMARY_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_telephone_2):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_BUSINESS2_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::mobile_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_MOBILE_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::radio_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_RADIO_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::car_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_CAR_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::other_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_OTHER_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::transmittable_display_name):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_TRANSMITABLE_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::pager_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_PAGER_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::primary_fax):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_PRIMARY_FAX_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_fax):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_BUSINESS_FAX_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_fax):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_HOME_FAX_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_address_country):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_COUNTRY_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_address_city):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_LOCALITY_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_address_state):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_STATE_OR_PROVINCE_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_address_street):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_STREET_ADDRESS_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_postal_code):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_POSTAL_CODE_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_po_box):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_POST_OFFICE_BOX_A));
		break;
	case static_cast<int>(schema::ContactProjection::telex_number):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_TELEX_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::isdn_number):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_ISDN_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::assistant_telephone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_ASSISTANT_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_telephone_2):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_HOME2_TELEPHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::assistant):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_ASSISTANT_A));
		break;
	case static_cast<int>(schema::ContactProjection::send_rich_info):
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, PR_SEND_RICH_INFO));
		break;
	case static_cast<int>(schema::ContactProjection::wedding_anniversary):
		output.SetValue(column_index, row_number,
		                from_prop<pstsdk::ulonglong>(col_type, prop_bag, PR_WEDDING_ANNIVERSARY));
		break;
	case static_cast<int>(schema::ContactProjection::birthday):
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, PR_BIRTHDAY));
		break;
	case static_cast<int>(schema::ContactProjection::hobbies):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_HOBBIES_A));
		break;
	case static_cast<int>(schema::ContactProjection::middle_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_MIDDLE_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::display_name_prefix):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_DISPLAY_NAME_PREFIX_A));
		break;
	case static_cast<int>(schema::ContactProjection::profession):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_PROFESSION_A));
		break;
	case static_cast<int>(schema::ContactProjection::preferred_by_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_PREFERRED_BY_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::spouse_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_SPOUSE_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::computer_network_name):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_COMPUTER_NETWORK_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::customer_id):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_CUSTOMER_ID_A));
		break;
	case static_cast<int>(schema::ContactProjection::ttytdd_phone):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_TTYTDD_PHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::ftp_site):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_FTP_SITE_A));
		break;
	case static_cast<int>(schema::ContactProjection::gender):
		output.SetValue(column_index, row_number, from_prop<int16_t>(col_type, prop_bag, PR_GENDER));
		break;
	case static_cast<int>(schema::ContactProjection::manager_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_MANAGER_NAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::nickname):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_NICKNAME_A));
		break;
	case static_cast<int>(schema::ContactProjection::personal_home_page):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_PERSONAL_HOME_PAGE_A));
		break;
	case static_cast<int>(schema::ContactProjection::business_home_page):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_BUSINESS_HOME_PAGE_A));
		break;
	case static_cast<int>(schema::ContactProjection::company_main_phone):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_COMPANY_MAIN_PHONE_NUMBER_A));
		break;
	case static_cast<int>(schema::ContactProjection::childrens_names):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_CHILDRENS_NAMES_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_address_city):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_HOME_ADDRESS_CITY_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_address_country):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_HOME_ADDRESS_COUNTRY_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_address_postal_code):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_HOME_ADDRESS_POSTAL_CODE_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_address_state):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_HOME_ADDRESS_STATE_OR_PROVINCE_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_address_street):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_HOME_ADDRESS_STREET_A));
		break;
	case static_cast<int>(schema::ContactProjection::home_address_po_box):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_HOME_ADDRESS_POST_OFFICE_BOX_A));
		break;
	case static_cast<int>(schema::ContactProjection::other_address_city):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_OTHER_ADDRESS_CITY_A));
		break;
	case static_cast<int>(schema::ContactProjection::other_address_country):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_OTHER_ADDRESS_COUNTRY_A));
		break;
	case static_cast<int>(schema::ContactProjection::other_address_postal_code):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_OTHER_ADDRESS_POSTAL_CODE_A));
		break;
	case static_cast<int>(schema::ContactProjection::other_address_state):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_OTHER_ADDRESS_STATE_OR_PROVINCE_A));
		break;
	case static_cast<int>(schema::ContactProjection::other_address_street):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_OTHER_ADDRESS_STREET_A));
		break;
	case static_cast<int>(schema::ContactProjection::other_address_po_box):
		output.SetValue(column_index, row_number,
		                from_prop<std::string>(col_type, prop_bag, PR_OTHER_ADDRESS_POST_OFFICE_BOX_A));
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                       pst::TypedBag<pst::MessageClass::Appointment> &appointment, idx_t row_number,
                       idx_t column_index) {
	auto &prop_bag = appointment.bag;
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	prop_id named_prop_id = 0;

	switch (schema_col) {
	case static_cast<int>(schema::AppointmentProjection::location):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidLocation_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::start_time):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidAppointmentStartWhole);
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::end_time):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidAppointmentEndWhole);
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::duration):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidAppointmentDuration);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::all_day_event):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidAppointmentSubType);
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::busy_status):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidBusyStatus);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::meeting_workspace_url):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidMeetingWorkspaceUrl_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::organizer_name):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidOwnerName_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::required_attendees):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidToAttendeesString_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::optional_attendees):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidCcAttendeesString_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::is_recurring):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidRecurring);
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::recurrence_pattern):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidRecurrencePattern_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::is_private): {
		// Using PR_SENSITIVITY to determine if private (2 = PRIVATE, 3 = CONFIDENTIAL)
		auto sensitivity = from_prop<int32_t>(LogicalType::INTEGER, prop_bag, PR_SENSITIVITY);
		if (!sensitivity.IsNull()) {
			auto val = sensitivity.GetValue<int32_t>();
			output.SetValue(column_index, row_number, Value::BOOLEAN(val >= 2));
		} else {
			output.SetValue(column_index, row_number, Value(nullptr));
		}
		break;
	}
	case static_cast<int>(schema::AppointmentProjection::response_status):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidResponseStatus);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::AppointmentProjection::is_meeting):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_appointment, PidLidFInvited);
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, named_prop_id));
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                       pst::TypedBag<pst::MessageClass::StickyNote> &sticky_note, idx_t row_number,
                       idx_t column_index) {
	auto &prop_bag = sticky_note.bag;
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	prop_id named_prop_id = 0;

	switch (schema_col) {
	case static_cast<int>(schema::StickyNoteProjection::note_color):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_note, PidLidNoteColor);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::StickyNoteProjection::note_width):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_note, PidLidNoteWidth);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::StickyNoteProjection::note_height):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_note, PidLidNoteHeight);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::StickyNoteProjection::note_x):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_note, PidLidNoteX);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::StickyNoteProjection::note_y):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_note, PidLidNoteY);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                       pst::TypedBag<pst::MessageClass::Task> &task, idx_t row_number, idx_t column_index) {
	auto &prop_bag = task.bag;
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	prop_id named_prop_id = 0;

	switch (schema_col) {
	case static_cast<int>(schema::TaskProjection::task_status):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskStatus);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::percent_complete):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidPercentComplete);
		output.SetValue(column_index, row_number, from_prop<double>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::is_team_task):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTeamTask);
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::start_date):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskStartDate);
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::due_date):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskDueDate);
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::date_completed):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskDateCompleted);
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::actual_effort):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskActualEffort);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::estimated_effort):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskEstimatedEffort);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::is_complete):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskComplete);
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::task_owner):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskOwner_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::task_assigner):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskAssigner_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::last_user):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskLastUser_A);
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::is_recurring):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskFRecurring);
		output.SetValue(column_index, row_number, from_prop<bool>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::ownership):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskOwnership);
		output.SetValue(column_index, row_number, from_prop<int32_t>(col_type, prop_bag, named_prop_id));
		break;
	case static_cast<int>(schema::TaskProjection::last_update):
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_task, PidLidTaskLastUpdate);
		output.SetValue(column_index, row_number, from_prop<pstsdk::ulonglong>(col_type, prop_bag, named_prop_id));
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                       pst::TypedBag<pst::MessageClass::Note, pstsdk::folder> &folder, idx_t row_number,
                       idx_t column_index) {
	auto &prop_bag = folder.bag;
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	switch (schema_col) {
	case static_cast<int>(schema::FolderProjection::container_class):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_CONTAINER_CLASS_A));
		break;
	case static_cast<int>(schema::FolderProjection::display_name):
		output.SetValue(column_index, row_number, from_prop<std::string>(col_type, prop_bag, PR_DISPLAY_NAME_A));
		break;
	case static_cast<int>(schema::FolderProjection::subfolder_count):
		output.SetValue(column_index, row_number, Value::UINTEGER(folder.sdk_object->get_subfolder_count()));
		break;
	case static_cast<int>(schema::FolderProjection::message_count):
		output.SetValue(column_index, row_number, Value::BIGINT(folder.sdk_object->get_message_count()));
		break;
	case static_cast<int>(schema::FolderProjection::unread_message_count):
		output.SetValue(column_index, row_number, Value::BIGINT(folder.sdk_object->get_unread_message_count()));
		break;
	default:
		break;
	}
}

template <>
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                       pst::TypedBag<pst::MessageClass::DistList> &dlist, idx_t row_number, idx_t column_index) {
	auto &prop_bag = dlist.bag;
	auto schema_col = local_state.column_ids()[column_index];
	auto &col_type = StructType::GetChildType(local_state.output_schema(), schema_col);

	prop_id named_prop_id = 0;
	switch (schema_col) {
	case static_cast<int>(schema::DistributionListProjection::one_off_members): {
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_address, PidLidDistributionListOneOffMembers);

		if (!prop_bag.prop_exists(named_prop_id)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			break;
		}

		auto entry_ids = prop_bag.read_prop_array<std::vector<pstsdk::byte>>(named_prop_id);
		vector<Value> oneoff_recipients;
		for (auto &entry : entry_ids) {
			auto header = reinterpret_cast<pstsdk::recipient_oneoff_entry_id *>(&entry.data()[0]);
			if (!pstsdk::guid_eq(header->provider_uid, pstsdk::provider_uid_recipient_oneoff)) {
				throw InvalidInputException(
				    "Unknown DistributionList entry ProviderUID, only One-Off entries are supported for this property");
			}

			vector<Value> one_off_recipient;
			for (auto &s : header->read_strings()) {
				one_off_recipient.emplace_back(Value(s));
			}

			oneoff_recipients.emplace_back(Value::STRUCT(schema::ONE_OFF_RECIPIENT_SCHEMA, one_off_recipient));
		}

		output.SetValue(column_index, row_number, Value::LIST(schema::ONE_OFF_RECIPIENT_SCHEMA, oneoff_recipients));
		break;
	}
	case static_cast<int>(schema::DistributionListProjection::member_node_ids): {
		named_prop_id = local_state.pst->lookup_prop_id(pstsdk::ps_address, PidLidDistributionListMembers);

		if (!prop_bag.prop_exists(named_prop_id)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			break;
		}

		auto entry_ids = prop_bag.read_prop_array<std::vector<pstsdk::byte>>(named_prop_id);
		vector<duckdb::Value> contact_nids;

		for (auto &entry : entry_ids) {
			auto header = reinterpret_cast<pstsdk::distribution_list_wrapped_entry_id *>(&entry.data()[0]);

			if (!pstsdk::guid_eq(header->provider_uid, pstsdk::provider_uid_wrapped_entry_id)) {
				throw InvalidInputException(
				    "Unknown DistributionList entry ProviderUID, only WrappedEntryId supported");
			}

			if (header->get_type() != pstsdk::distribution_list_entry_id_type::contact) {
				throw InvalidInputException("Only contact entries are supported");
			}

			// TODO: In a PST file, this is the standard 24 byte entry ID format (last 4 bytes nid)
			// but it could be a "Message EntryID Structure" which is different
			// https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxocntc/02656215-1cb0-4b06-a077-b07e756216be
			pstsdk::node_id contact_nid;
			memcpy(&contact_nid, &header->data[20], sizeof(pstsdk::node_id));
			contact_nids.emplace_back(Value::UINTEGER(contact_nid));
		}

		output.SetValue(column_index, row_number, Value::LIST(contact_nids));
		break;
	}
	default:
		break;
	}
}

template <typename Item>
void into_row(PSTReadLocalState &local_state, duckdb::DataChunk &output, Item &item, idx_t row_number) {
	for (idx_t col_idx = 0; col_idx < local_state.column_ids().size(); ++col_idx) {
		auto schema_col = local_state.column_ids()[col_idx];

		// Bind virtual columns + node_ids (should be 'infallible' as long as the file isn't borked)
		switch (schema_col) {
		case static_cast<int>(schema::PSTProjection::node_id):
		case schema::PST_VCOL_NODE_ID:
			output.SetValue(col_idx, row_number, Value::UINTEGER(item.nid));
			break;
		case static_cast<int>(schema::PSTProjection::parent_node_id):
			output.SetValue(col_idx, row_number,
			                Value::UINTEGER(item.sdk_object->get_property_bag().get_node().get_parent_id()));
			break;
		case schema::PST_VCOL_PARTITION_INDEX:
			output.SetValue(col_idx, row_number, Value::UBIGINT(local_state.partition->partition_index));
			break;
		default:
			try {
				// Bind PST attributes
				set_output_column<pstsdk::pst>(local_state, output, *local_state.pst, row_number, col_idx);

				// If message-like, bind IPM.Note base attributes
				if constexpr (!pst::is_folder_bag_v<Item>) {
					set_output_column<pstsdk::message>(local_state, output, *item.sdk_object, row_number, col_idx);
				}

				// If reading as note, we are already done
				if constexpr (pst::is_base_msg_bag_v<Item>)
					continue;

				set_output_column(local_state, output, item, row_number, col_idx);
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

template void into_row<pst::TypedBag<pst::MessageClass::Note, pstsdk::folder>>(
    PSTReadLocalState &local_state, duckdb::DataChunk &output,
    pst::TypedBag<pst::MessageClass::Note, pstsdk::folder> &item, idx_t row_number);

template void into_row<pst::TypedBag<pst::MessageClass::Note>>(PSTReadLocalState &local_state,
                                                               duckdb::DataChunk &output,
                                                               pst::TypedBag<pst::MessageClass::Note> &item,
                                                               idx_t row_number);

template void
into_row<pst::TypedBag<pst::MessageClass::Appointment>>(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                                                        pst::TypedBag<pst::MessageClass::Appointment> &item,
                                                        idx_t row_number);

template void into_row<pst::TypedBag<pst::MessageClass::Contact>>(PSTReadLocalState &local_state,
                                                                  duckdb::DataChunk &output,
                                                                  pst::TypedBag<pst::MessageClass::Contact> &item,
                                                                  idx_t row_number);

template void into_row<pst::TypedBag<pst::MessageClass::StickyNote>>(PSTReadLocalState &local_state,
                                                                     duckdb::DataChunk &output,
                                                                     pst::TypedBag<pst::MessageClass::StickyNote> &item,
                                                                     idx_t row_number);

template void into_row<pst::TypedBag<pst::MessageClass::Task>>(PSTReadLocalState &local_state,
                                                               duckdb::DataChunk &output,
                                                               pst::TypedBag<pst::MessageClass::Task> &item,
                                                               idx_t row_number);

template void into_row<pst::TypedBag<pst::MessageClass::DistList>>(PSTReadLocalState &local_state,
                                                                   duckdb::DataChunk &output,
                                                                   pst::TypedBag<pst::MessageClass::DistList> &item,
                                                                   idx_t row_number);

} // namespace intellekt::duckpst::row_serializer

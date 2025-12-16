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
#include "pst/typed_bag.hpp"
#include "table_function.hpp"

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
void set_output_column(PSTReadLocalState &local_state, duckdb::DataChunk &output,
                       pst::TypedBag<pst::MessageClass::Note> &msg, idx_t row_number, idx_t column_index) {
	auto &prop_bag = msg.bag;
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
		output.SetValue(column_index, row_number, Value::UBIGINT(msg.sdk_object->size()));
		break;
	case static_cast<int>(schema::MessageProjection::has_attachments): {
		size_t attachment_count = msg.sdk_object->get_attachment_count();
		output.SetValue(column_index, row_number, Value::BOOLEAN(attachment_count > 0));
		break;
	}
	case static_cast<int>(schema::MessageProjection::attachment_count): {
		size_t attachment_count = msg.sdk_object->get_attachment_count();
		output.SetValue(column_index, row_number, Value::UBIGINT(attachment_count));
		break;
	}
	case static_cast<int>(schema::MessageProjection::body):
		if (!prop_bag.prop_exists(PR_BODY_A)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			return;
		}

		if (read_size == 0)
			read_size = msg.sdk_object->body_size();
		read_size = std::min<idx_t>(read_size, msg.sdk_object->body_size());
		output.SetValue(column_index, row_number,
		                from_prop_stream<std::string>(col_type, prop_bag, PR_BODY_A, read_size));
		break;
	case static_cast<int>(schema::MessageProjection::body_html):
		if (!prop_bag.prop_exists(PR_HTML)) {
			output.SetValue(column_index, row_number, Value(nullptr));
			return;
		}

		if (read_size == 0)
			read_size = msg.sdk_object->html_body_size();
		read_size = std::min<idx_t>(read_size, msg.sdk_object->html_body_size());
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
		for (auto it = msg.sdk_object->recipient_begin(); it != msg.sdk_object->recipient_end(); ++it) {
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
		for (auto it = msg.sdk_object->attachment_begin(); it != msg.sdk_object->attachment_end(); ++it) {
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
                       pst::TypedBag<pst::MessageClass::Note, pstsdk::folder> &folder, idx_t row_number,
                       idx_t column_index) {
	auto &prop_bag = folder.bag;
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

template <typename Item>
void into_row(PSTReadLocalState &local_state, duckdb::DataChunk &output, Item &item, idx_t row_number) {
	for (idx_t col_idx = 0; col_idx < local_state.column_ids().size(); ++col_idx) {
		auto schema_col = local_state.column_ids()[col_idx];

		// Bind virtual columns + node_id (should be 'infallible' as long as the file isn't borked)
		switch (schema_col) {
		case static_cast<int>(schema::PSTProjection::node_id):
		case schema::PST_ITEM_NODE_ID:
			output.SetValue(col_idx, row_number, Value::UINTEGER(item.node.get_id()));
			break;
		case schema::PST_PARTITION_INDEX:
			output.SetValue(col_idx, row_number, Value::UBIGINT(local_state.partition->partition_index));
			break;
		default:
			try {
				// Bind PST attributes
				set_output_column<pstsdk::pst>(local_state, output, *local_state.pst, row_number, col_idx);

				// If message-like, bind common message attributes
				if constexpr (!pst::is_folder_bag_v<Item>) {
					// Bind common columns (message only)
					set_output_column<const_property_object>(local_state, output, item.bag, row_number, col_idx);
				}

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

template void into_row<pst::TypedBag<pst::MessageClass::Contact>>(PSTReadLocalState &local_state,
                                                                  duckdb::DataChunk &output,
                                                                  pst::TypedBag<pst::MessageClass::Contact> &item,
                                                                  idx_t row_number);

} // namespace intellekt::duckpst::row_serializer

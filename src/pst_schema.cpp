#include "pst_schema.hpp"
#include "pstsdk/pst/folder.h"
#include "utils.hpp"

namespace intellekt::duckpst::schema {
template <>
void into_row<pstsdk::folder>(PSTIteratorLocalTableFunctionState &local_state, DataChunk &output,
                              pstsdk::folder &folder, idx_t row_number) {
	output.SetValue(0, row_number, Value(local_state.current_file().path));
	output.SetValue(1, row_number, Value(utils::to_utf8(local_state.current_pst()->get_name())));
	output.SetValue(2, row_number, Value::UINTEGER(folder.get_property_bag().get_node().get_parent_id()));
	output.SetValue(3, row_number, Value::UINTEGER(folder.get_id()));
	output.SetValue(4, row_number, Value(utils::to_utf8(folder.get_name())));
	output.SetValue(5, row_number, Value::UINTEGER(folder.get_subfolder_count()));
	output.SetValue(6, row_number, Value::BIGINT(folder.get_message_count()));
	output.SetValue(7, row_number, Value::BIGINT(folder.get_unread_message_count()));
}

template <>
void into_row<pstsdk::message>(PSTIteratorLocalTableFunctionState &local_state, DataChunk &output, pstsdk::message &msg,
                               idx_t row_number) {
	auto &prop_bag = msg.get_property_bag();

	output.SetValue(0, row_number, Value(local_state.current_file().path));
	output.SetValue(1, row_number, Value(utils::to_utf8(local_state.current_pst()->get_name())));
	output.SetValue(2, row_number, Value::UINTEGER(prop_bag.get_node().get_parent_id()));
	output.SetValue(3, row_number, Value::UINTEGER(msg.get_id()));

	// Subject
	output.SetValue(4, row_number, Value(utils::read_prop_utf8(prop_bag, 0x37)));

	// PidTagSenderName
	if (prop_bag.prop_exists(0x0C1A)) {
		output.SetValue(5, row_number, Value(utils::read_prop_utf8(prop_bag, 0x0C1A)));
	} else {
		output.SetValue(5, row_number, Value(nullptr));
	}

	// PidTagSenderAddress
	if (prop_bag.prop_exists(0x0C1F)) {
		output.SetValue(6, row_number, Value(utils::read_prop_utf8(prop_bag, 0x0C1F)));
	} else {
		output.SetValue(6, row_number, Value(nullptr));
	}

	// PidTagMessageDeliveryTime
	if (prop_bag.prop_exists(0x0E06)) {
		auto filetime = prop_bag.read_prop<pstsdk::ulonglong>(0x0E06);
		time_t unixtime = pstsdk::filetime_to_time_t(filetime);
		output.SetValue(7, row_number, Value::TIMESTAMP(timestamp_sec_t(unixtime)));
	} else {
		output.SetValue(7, row_number, Value(nullptr));
	}

	// PidTagMessageClass
	if (prop_bag.prop_exists(0x001A)) {
		output.SetValue(8, row_number, Value(utils::read_prop_utf8(prop_bag, 0x001A)));
	} else {
		output.SetValue(8, row_number, Value(nullptr));
	}

	// PidTagImportance (defaults to 'normal')
	if (prop_bag.prop_exists(0x0017)) {
		uint32_t importance = prop_bag.read_prop<uint32_t>(0x0017);
		output.SetValue(9, row_number, Value::ENUM(importance, schema::IMPORTANCE_ENUM));
	} else {
		output.SetValue(9, row_number, Value(nullptr));
	}

	// PidTagSensitivity (defaults to 'none')
	if (prop_bag.prop_exists(0x0036)) {
		uint32_t sensitivity = prop_bag.read_prop<uint32_t>(0x0036);
		output.SetValue(10, row_number, Value::ENUM(sensitivity, schema::SENSITIVITY_ENUM));
	} else {
		output.SetValue(10, row_number, Value(nullptr));
	}

	// PidTagMessageFlags (bitmask)
	if (prop_bag.prop_exists(0x0E07)) {
		output.SetValue(11, row_number, Value::UINTEGER(prop_bag.read_prop<uint32_t>(0x0E07)));
	} else {
		output.SetValue(11, row_number, Value(nullptr));
	}

	output.SetValue(12, row_number, Value::UINTEGER(msg.size()));

	size_t attachment_count = msg.get_attachment_count();
	output.SetValue(13, row_number, Value::BOOLEAN(attachment_count > 0));
	output.SetValue(14, row_number, Value::UINTEGER(attachment_count));

	// Re-encode plaintext as UTF-8
	try {
		std::wstring body = msg.get_body();
		output.SetValue(15, row_number, Value(utils::to_utf8(body)));
	} catch (...) {
		output.SetValue(15, row_number, Value(nullptr));
	}

	// Re-encode HTML body as UTF-8
	try {
		std::wstring html_body = msg.get_html_body();
		output.SetValue(16, row_number, Value(utils::to_utf8(html_body)));
	} catch (...) {
		output.SetValue(16, row_number, Value(nullptr));
	}

	// PidTagInternetMessageId
	if (prop_bag.prop_exists(0x1035)) {
		output.SetValue(17, row_number, Value(utils::read_prop_utf8(prop_bag, 0x1035)));
	} else {
		output.SetValue(17, row_number, Value(nullptr));
	}

	// PidTagConversationTopic
	if (prop_bag.prop_exists(0x0070)) {
		output.SetValue(18, row_number, Value(utils::read_prop_utf8(prop_bag, 0x0070)));
	} else {
		output.SetValue(18, row_number, Value(nullptr));
	}

	// Recipients
	vector<Value> recipients;
	for (auto it = msg.recipient_begin(); it != msg.recipient_end(); ++it) {
		auto recipient = *it;
		auto recipient_prop_bag = recipient.get_property_row();

		vector<Value> values;

		Value account_name;
		if (recipient.has_account_name()) {
			account_name = Value(utils::read_prop_utf8(recipient_prop_bag, 0x3a00));
		} else {
			account_name = Value(nullptr);
		}

		Value email_address;
		if (recipient.has_email_address()) {
			email_address = Value(utils::read_prop_utf8(recipient_prop_bag, 0x39fe));
		} else {
			email_address = Value(nullptr);
		}

		// Recipient display name
		values.emplace_back(Value(utils::read_prop_utf8(recipient_prop_bag, 0x3001)));
		values.emplace_back(account_name);
		values.emplace_back(email_address);

		// Address type
		values.emplace_back(Value(utils::read_prop_utf8(recipient_prop_bag, 0x3002)));
		values.emplace_back(Value::ENUM(recipient.get_type(), schema::RECIPIENT_TYPE_ENUM));

		recipients.emplace_back(Value::STRUCT(schema::RECIPIENT_SCHEMA, values));
	}
	output.SetValue(19, row_number, Value::LIST(schema::RECIPIENT_SCHEMA, recipients));

	vector<Value> attachments;
	// for (auto it = msg.attachment_begin(); it != msg.attachment_end(); ++it) {
	//     auto attachment = *it;
	//     auto attachment_prop_bag = attachment.get_property_bag();

	//     vector<Value> values;
	//     try {
	//         values.emplace_back(Value(utils::read_prop_utf8(attachment_prop_bag, 0x3707)));
	//     } catch (...) {
	//         values.emplace_back(Value(utils::read_prop_utf8(attachment_prop_bag, 0x3704)));
	//     }
	//     values.emplace_back(Value::UBIGINT(attachment.size()));
	//     values.emplace_back(Value(attachment.is_message()));

	//     auto attachment_bytes = attachment.get_bytes();
	//     values.emplace_back(Value::BLOB_RAW({attachment_bytes.begin(), attachment_bytes.end()}));

	//     attachments.emplace_back(Value::STRUCT(schema::ATTACHMENT_SCHEMA, values));
	// }

	output.SetValue(20, row_number, Value::LIST(schema::ATTACHMENT_SCHEMA, attachments));
}
} // namespace intellekt::duckpst::schema

#include "pst_schema.hpp"
#include "pstsdk/pst/folder.h"
#include "utils.hpp"

namespace intellekt::duckpst::schema {
template <>
void into_row<pstsdk::folder>(PSTIteratorLocalTableFunctionState &local_state, DataChunk &output,
                              pstsdk::folder &folder, idx_t row_number) {
	auto &column_ids = local_state.column_ids();
	for (idx_t col_idx = 0; col_idx < column_ids.size(); ++col_idx) {
		auto col = column_ids[col_idx];
		switch (col) {
		case 0:
			output.SetValue(col, row_number, Value(local_state.current_file().path));
			break;
		case 1:
			output.SetValue(col, row_number, Value(utils::to_utf8(local_state.current_pst()->get_name())));
			break;
		case 2:
			output.SetValue(col, row_number, Value::UINTEGER(folder.get_property_bag().get_node().get_parent_id()));
			break;
		case 3:
			output.SetValue(col, row_number, Value::UINTEGER(folder.get_id()));
			break;
		case 4:
			output.SetValue(col, row_number, Value(utils::to_utf8(folder.get_name())));
			break;
		case 5:
			output.SetValue(col, row_number, Value::UINTEGER(folder.get_subfolder_count()));
			break;
		case 6:
			output.SetValue(col, row_number, Value::BIGINT(folder.get_message_count()));
			break;
		case 7:
			output.SetValue(col, row_number, Value::BIGINT(folder.get_unread_message_count()));
			break;
		default:
			break;
		}
	}
}

template <>
void into_row<pstsdk::message>(PSTIteratorLocalTableFunctionState &local_state, DataChunk &output, pstsdk::message &msg,
                               idx_t row_number) {
	auto &column_ids = local_state.column_ids();
	auto &prop_bag = msg.get_property_bag();

	for (idx_t col_idx = 0; col_idx < column_ids.size(); ++col_idx) {
		auto col = column_ids[col_idx];

		switch (col) {
		case 0:
			output.SetValue(col_idx, row_number, Value(local_state.current_file().path));
			break;
		case 1:
			output.SetValue(col_idx, row_number, Value(utils::to_utf8(local_state.current_pst()->get_name())));
			break;
		case 2:
			output.SetValue(col_idx, row_number, Value::UINTEGER(prop_bag.get_node().get_parent_id()));
			break;
		case 3:
			output.SetValue(col_idx, row_number, Value::UINTEGER(msg.get_id()));
			break;
		case 4:
			// Subject
			output.SetValue(col_idx, row_number, Value(utils::read_prop_utf8(prop_bag, 0x37)));
			break;
		case 5:
			// PidTagSenderName
			if (prop_bag.prop_exists(0x0C1A)) {
				output.SetValue(col_idx, row_number, Value(utils::read_prop_utf8(prop_bag, 0x0C1A)));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 6:
			// PidTagSenderAddress
			if (prop_bag.prop_exists(0x0C1F)) {
				output.SetValue(col_idx, row_number, Value(utils::read_prop_utf8(prop_bag, 0x0C1F)));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 7:
			// PidTagMessageDeliveryTime
			if (prop_bag.prop_exists(0x0E06)) {
				auto filetime = prop_bag.read_prop<pstsdk::ulonglong>(0x0E06);
				time_t unixtime = pstsdk::filetime_to_time_t(filetime);
				output.SetValue(col_idx, row_number, Value::TIMESTAMP(timestamp_sec_t(unixtime)));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 8:
			// PidTagMessageClass
			if (prop_bag.prop_exists(0x001A)) {
				output.SetValue(col_idx, row_number, Value(utils::read_prop_utf8(prop_bag, 0x001A)));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 9:
			// PidTagImportance (defaults to 'normal')
			if (prop_bag.prop_exists(0x0017)) {
				uint32_t importance = prop_bag.read_prop<uint32_t>(0x0017);
				output.SetValue(col_idx, row_number, Value::ENUM(importance, schema::IMPORTANCE_ENUM));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 10:
			// PidTagSensitivity (defaults to 'none')
			if (prop_bag.prop_exists(0x0036)) {
				uint32_t sensitivity = prop_bag.read_prop<uint32_t>(0x0036);
				output.SetValue(col_idx, row_number, Value::ENUM(sensitivity, schema::SENSITIVITY_ENUM));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 11:
			// PidTagMessageFlags (bitmask)
			if (prop_bag.prop_exists(0x0E07)) {
				output.SetValue(col_idx, row_number, Value::UINTEGER(prop_bag.read_prop<uint32_t>(0x0E07)));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 12:
			output.SetValue(col_idx, row_number, Value::UINTEGER(msg.size()));
			break;
		case 13: {
			size_t attachment_count = msg.get_attachment_count();
			output.SetValue(col_idx, row_number, Value::BOOLEAN(attachment_count > 0));
			break;
		}
		case 14: {
			size_t attachment_count = msg.get_attachment_count();
			output.SetValue(col_idx, row_number, Value::UINTEGER(attachment_count));
			break;
		}
		case 15:
			// Re-encode plaintext as UTF-8
			try {
				std::wstring body = msg.get_body();
				output.SetValue(col_idx, row_number, Value(utils::to_utf8(body)));
			} catch (...) {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 16:
			// Re-encode HTML body as UTF-8
			try {
				std::wstring html_body = msg.get_html_body();
				output.SetValue(col_idx, row_number, Value(utils::to_utf8(html_body)));
			} catch (...) {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 17:
			// PidTagInternetMessageId
			if (prop_bag.prop_exists(0x1035)) {
				output.SetValue(col_idx, row_number, Value(utils::read_prop_utf8(prop_bag, 0x1035)));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 18:
			// PidTagConversationTopic
			if (prop_bag.prop_exists(0x0070)) {
				output.SetValue(col_idx, row_number, Value(utils::read_prop_utf8(prop_bag, 0x0070)));
			} else {
				output.SetValue(col_idx, row_number, Value(nullptr));
			}
			break;
		case 19: {
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
			output.SetValue(col_idx, row_number, Value::LIST(schema::RECIPIENT_SCHEMA, recipients));
			break;
		}
		case 20: {
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

			output.SetValue(col_idx, row_number, Value::LIST(schema::ATTACHMENT_SCHEMA, attachments));
			break;
		}
		default:
			break;
		}
	}
}
} // namespace intellekt::duckpst::schema

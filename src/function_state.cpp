#include "function_state.hpp"
#include "duckdb/common/exception.hpp"
#include "pst_schema.hpp"
#include "pstsdk/pst/message.h"
#include "pstsdk/util/util.h"
#include "table_function.hpp"
#include "utils.hpp"

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// PSTReadGlobalTableFunctionState
PSTReadGlobalTableFunctionState::PSTReadGlobalTableFunctionState(queue<OpenFileInfo> &&files,
                                                                 const PSTReadFunctionMode mode,
                                                                 const LogicalType &output_schema)
    : files(boost::synchronized_value<queue<OpenFileInfo>>(files)), mode(mode), output_schema(output_schema) {
}

idx_t PSTReadGlobalTableFunctionState::MaxThreads() const {
	return files->size();
}

std::optional<OpenFileInfo> PSTReadGlobalTableFunctionState::take() {
	auto sync_files = files.synchronize();
	if (sync_files->empty())
		return {};

	auto front = sync_files->front();

	sync_files->pop();
	return front;
}

// PSTIteratorLocalTableFunctionState
PSTIteratorLocalTableFunctionState::PSTIteratorLocalTableFunctionState(OpenFileInfo &&file,
                                                                       PSTReadGlobalTableFunctionState &global_state)
    : file(file), global_state(global_state) {
}

template <typename it, typename t>
PSTConcreteIteratorState<it, t>::PSTConcreteIteratorState(OpenFileInfo &&file,
                                                          PSTReadGlobalTableFunctionState &global_state)
    : PSTIteratorLocalTableFunctionState(std::move(file), global_state) {
	pst.emplace(pstsdk::pst(utils::to_wstring(file.path)));
	bind_iter();
}

template <typename it, typename t>
std::optional<t> PSTConcreteIteratorState<it, t>::next() {
	if (finished() && !bind_next())
		return {};
	t x = **current;
	++(*current);
	return x;
}

template <typename it, typename t>
bool PSTConcreteIteratorState<it, t>::bind_next() {
	if (!finished())
		return false;
	auto next_file = global_state.take();
	if (!next_file)
		return false;

	file = std::move(*next_file);
	pst.emplace(pstsdk::pst(utils::to_wstring(file.path)));

	bind_iter();

	return true;
}

template <typename it, typename t>
const bool PSTConcreteIteratorState<it, t>::finished() {
	return (!current) || (current == end);
}

template <typename it, typename t>
const std::optional<class pst> &PSTConcreteIteratorState<it, t>::current_pst() {
	return pst;
}

template <typename it, typename t>
const OpenFileInfo &PSTConcreteIteratorState<it, t>::current_file() {
	return file;
}

template <typename it, typename t>
const LogicalType &PSTConcreteIteratorState<it, t>::output_schema() {
	return global_state.output_schema;
}

// PSTIteratorLocalTableFunctionState (NDB Folders)
template <>
void PSTConcreteIteratorState<pst::folder_iterator, folder>::bind_iter() {
	current = pst->folder_begin();
	end = pst->folder_end();
}

template <>
idx_t PSTConcreteIteratorState<pst::folder_iterator, folder>::emit_rows(DataChunk &output) {
	idx_t rows = 0;

	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		auto folder = next();
		if (!folder) {
			break;
		}

		output.SetValue(0, i, Value(current_file().path));
		output.SetValue(1, i, Value(utils::to_utf8(current_pst()->get_name())));
		output.SetValue(2, i, Value::UINTEGER(folder->get_property_bag().get_node().get_parent_id()));
		output.SetValue(3, i, Value::UINTEGER(folder->get_id()));
		output.SetValue(4, i, Value(utils::to_utf8(folder->get_name())));
		output.SetValue(5, i, Value::UINTEGER(folder->get_subfolder_count()));
		output.SetValue(6, i, Value::BIGINT(folder->get_message_count()));
		output.SetValue(7, i, Value::BIGINT(folder->get_unread_message_count()));

		++rows;
	}

	return rows;
}

// PSTIteratorLocalTableFunctionState (NDB Messages)
template <>
void PSTConcreteIteratorState<pst::message_iterator, message>::bind_iter() {
	current = pst->message_begin();
	end = pst->message_end();
}

template <>
idx_t PSTConcreteIteratorState<pst::message_iterator, message>::emit_rows(DataChunk &output) {
	idx_t rows = 0;

	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		auto msg = next();
		if (!msg) {
			break;
		}

		auto &prop_bag = msg->get_property_bag();

		output.SetValue(0, i, Value(current_file().path));
		output.SetValue(1, i, Value(utils::to_utf8(current_pst()->get_name())));
		output.SetValue(2, i, Value::UINTEGER(prop_bag.get_node().get_parent_id()));
		output.SetValue(3, i, Value::UINTEGER(msg->get_id()));

		// Subject
		output.SetValue(4, i, Value(utils::read_prop_utf8(prop_bag, 0x37)));

		// PidTagSenderName
		if (prop_bag.prop_exists(0x0C1A)) {
			output.SetValue(5, i, Value(utils::read_prop_utf8(prop_bag, 0x0C1A)));
		} else {
			output.SetValue(5, i, Value(nullptr));
		}

		// PidTagSenderAddress
		if (prop_bag.prop_exists(0x0C1F)) {
			output.SetValue(6, i, Value(utils::read_prop_utf8(prop_bag, 0x0C1F)));
		} else {
			output.SetValue(6, i, Value(nullptr));
		}

		// PidTagMessageDeliveryTime
		if (prop_bag.prop_exists(0x0E06)) {
			auto filetime = prop_bag.read_prop<ulonglong>(0x0E06);
			time_t unixtime = filetime_to_time_t(filetime);
			output.SetValue(7, i, Value::TIMESTAMP(timestamp_sec_t(unixtime)));
		} else {
			output.SetValue(7, i, Value(nullptr));
		}

		// PidTagMessageClass
		if (prop_bag.prop_exists(0x001A)) {
			output.SetValue(8, i, Value(utils::read_prop_utf8(prop_bag, 0x001A)));
		} else {
			output.SetValue(8, i, Value(nullptr));
		}

		// PidTagImportance (defaults to 'normal')
		if (prop_bag.prop_exists(0x0017)) {
			uint32_t importance = prop_bag.read_prop<uint32_t>(0x0017);
			output.SetValue(9, i, Value::ENUM(importance, schema::IMPORTANCE_ENUM));
		} else {
			output.SetValue(9, i, Value(nullptr));
		}

		// PidTagSensitivity (defaults to 'none')
		if (prop_bag.prop_exists(0x0036)) {
			uint32_t sensitivity = prop_bag.read_prop<uint32_t>(0x0036);
			output.SetValue(10, i, Value::ENUM(sensitivity, schema::SENSITIVITY_ENUM));
		} else {
			output.SetValue(10, i, Value(nullptr));
		}

		// PidTagMessageFlags (bitmask)
		if (prop_bag.prop_exists(0x0E07)) {
			output.SetValue(11, i, Value::UINTEGER(prop_bag.read_prop<uint32_t>(0x0E07)));
		} else {
			output.SetValue(11, i, Value(nullptr));
		}

		output.SetValue(12, i, Value::UINTEGER(msg->size()));

		size_t attachment_count = msg->get_attachment_count();
		output.SetValue(13, i, Value::BOOLEAN(attachment_count > 0));
		output.SetValue(14, i, Value::UINTEGER(attachment_count));

		// Re-encode plaintext as UTF-8
		try {
			std::wstring body = msg->get_body();
			output.SetValue(15, i, Value(utils::to_utf8(body)));
		} catch (...) {
			output.SetValue(15, i, Value(nullptr));
		}

		// Re-encode HTML body as UTF-8
		try {
			std::wstring html_body = msg->get_html_body();
			output.SetValue(16, i, Value(utils::to_utf8(html_body)));
		} catch (...) {
			output.SetValue(16, i, Value(nullptr));
		}

		// PidTagInternetMessageId
		if (prop_bag.prop_exists(0x1035)) {
			output.SetValue(17, i, Value(utils::read_prop_utf8(prop_bag, 0x1035)));
		} else {
			output.SetValue(17, i, Value(nullptr));
		}

		// PidTagConversationTopic
		if (prop_bag.prop_exists(0x0070)) {
			output.SetValue(18, i, Value(utils::read_prop_utf8(prop_bag, 0x0070)));
		} else {
			output.SetValue(18, i, Value(nullptr));
		}

        // Recipients
        vector<Value> recipients;
        for (auto it = msg->recipient_begin(); it != msg->recipient_end(); ++it) {
            auto recipient = *it;
            child_list_t<Value> recipient_struct;
            vector<Value> values;

            Value account_name;
            if (recipient.has_account_name()) {
                account_name = Value(utils::to_utf8(recipient.get_account_name()));
            } else {
                account_name = Value(nullptr);
            }

            Value email_address;
            if (recipient.has_email_address()) {
                email_address = Value(utils::to_utf8(recipient.get_email_address()));
            } else {
                email_address = Value(nullptr);
            }

            values.emplace_back(Value(utils::to_utf8(recipient.get_name())));
            values.emplace_back(account_name);
            values.emplace_back(email_address);
            values.emplace_back(Value(utils::to_utf8(recipient.get_address_type())));
            values.emplace_back(Value::ENUM(recipient.get_type(), schema::RECIPIENT_TYPE_ENUM));

            recipients.emplace_back(Value::STRUCT(schema::RECIPIENT_SCHEMA, values));
        }
        output.SetValue(19, i, Value::LIST(schema::RECIPIENT_SCHEMA, recipients));
		output.SetValue(20, i, Value(nullptr));

		++rows;
	}

	return rows;
}

template class PSTConcreteIteratorState<pst::folder_iterator, folder>;
template class PSTConcreteIteratorState<pst::message_iterator, message>;
} // namespace intellekt::duckpst

#include "function_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/vector_size.hpp"
#include "pst_schema.hpp"
#include "pstsdk/pst/folder.h"
#include "pstsdk/pst/message.h"
#include "pstsdk/util/primitives.h"
#include "table_function.hpp"
#include "utils.hpp"
#include <optional>
#include <utility>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// PSTReadGlobalTableFunctionState
PSTReadGlobalTableFunctionState::PSTReadGlobalTableFunctionState(const PSTReadTableFunctionData &bind_data,
                                                                 vector<column_t> column_ids)
    : mode(bind_data.mode), column_ids(std::move(column_ids)), total_files(bind_data.files.size()) {
	for (auto &file : bind_data.files) {
		files->push(file);
	}

	for (auto &folders : bind_data.pst_folders) {
		folder_ids.emplace(queue<node_id>({folders.begin(), folders.end()}));
	}

	// This isn't necessary, however it allows all local spoolers to begin
	// working immediately instead of blocking to hydrate the message IDs list
	if (mode == PSTReadFunctionMode::Message) {
		auto bound = 0;
		while (bound < 1) {
			bound = this->bind_message_ids();
			if (bound < 0)
				break;
		}
	}
}

int64_t PSTReadGlobalTableFunctionState::bind_message_ids() {
	auto sync_message_ids = current_message_ids.unique_synchronize();

	auto next = take_folder();
	if (!next)
		return -1;

	auto &[file, folder_id] = *next;

	auto pst = pstsdk::pst(utils::to_wstring(file.path));
	auto folder = pst.open_folder(folder_id);

	if (folder.get_message_count() < 1)
		return 0;

	queue<node_id> msgs;
	idx_t msg_count = 0;

	for (auto it = folder.message_begin(); it != folder.message_end(); ++it) {
		msgs.emplace(it->get_id());
		++msg_count;
	}

	total_messages = msgs.size();
	sync_message_ids->emplace(std::move(msgs));
	current_file.emplace(std::move(file));

	return msg_count;
}

double PSTReadGlobalTableFunctionState::progress() const {
	if (total_files < 1)
		return 100.0;

	double remain = files->size();
	if (current_file.has_value())
		++remain;

	return (100.0 * (1.0 - (remain / total_files)));
}

idx_t PSTReadGlobalTableFunctionState::MaxThreads() const {
	idx_t threads = 0;

	switch (this->mode) {
	case PSTReadFunctionMode::Folder:
		threads = this->files->size();
		break;
	case PSTReadFunctionMode::Message:
		if (this->current_message_ids.value().has_value()) {
			threads = (this->current_message_ids.value()->size() / DEFAULT_STANDARD_VECTOR_SIZE);
		} else if (!this->folder_ids.empty()) {
			threads = this->folder_ids.front().size();
		}
		break;
	default:
		break;
	}

	return std::max<idx_t>(threads, 1);
}

std::optional<OpenFileInfo> PSTReadGlobalTableFunctionState::take_file() {
	auto sync_files = files.unique_synchronize();
	if (sync_files->empty())
		return {};

	auto file = sync_files->front();
	sync_files->pop();
	return std::move(file);
};

std::optional<std::pair<OpenFileInfo, node_id>> PSTReadGlobalTableFunctionState::take_folder() {
	auto sync_files = files.unique_synchronize();

	if (sync_files->empty() || folder_ids.empty())
		return {};

	if (folder_ids.front().empty()) {
		folder_ids.pop();
		sync_files->pop();
		return take_folder();
	}

	auto file = sync_files->front();
	auto folder_id = folder_ids.front().front();
	folder_ids.front().pop();

	if (folder_ids.front().empty() || this->mode == PSTReadFunctionMode::Folder) {
		sync_files->pop();
		if (!folder_ids.empty())
			folder_ids.pop();
	}

	return std::make_pair(std::move(file), std::move(folder_id));
}

std::optional<std::pair<OpenFileInfo, vector<node_id>>> PSTReadGlobalTableFunctionState::take_messages(idx_t n) {
	if (!current_message_ids->has_value())
		return {};

	while (current_message_ids.value()->size() < 1) {
		if (bind_message_ids() < 0)
			break;
	}

	auto messages = current_message_ids.unique_synchronize();
	if (!messages || !messages->has_value())
		return {};

	if (messages->value().empty())
		return {};

	vector<node_id> batch;
	for (auto i = 0; i < n; ++i) {
		if (messages->value().empty())
			break;
		batch.push_back(messages->value().front());
		messages->value().pop();
	}

	return std::make_pair(*current_file, std::move(batch));
}

// PSTIteratorLocalTableFunctionState
PSTIteratorLocalTableFunctionState::PSTIteratorLocalTableFunctionState(PSTReadGlobalTableFunctionState &global_state)
    : global_state(global_state) {
}

const vector<column_t> &PSTIteratorLocalTableFunctionState::column_ids() {
	return global_state.column_ids;
}

// PSTConcreteIteratorState
template <typename it, typename t>
PSTConcreteIteratorState<it, t>::PSTConcreteIteratorState(PSTReadGlobalTableFunctionState &global_state)
    : PSTIteratorLocalTableFunctionState(global_state) {
}

template <typename it, typename t>
const bool PSTConcreteIteratorState<it, t>::finished() {
	return (!current) || (current == end);
}

template <typename it, typename t>
std::optional<t> PSTConcreteIteratorState<it, t>::next() {
	// If the current state is finished, keep going until we can keep binding
	while (finished() && bind_next()) {
	}

	// If we can't bind anymore and are finished, we're really finished
	if (finished())
		return {};

	t x = current_item();
	++(*current);
	return x;
}

template <typename it, typename t>
t PSTConcreteIteratorState<it, t>::current_item() {
	return **current;
}

template <>
message PSTConcreteIteratorState<vector<node_id>::iterator, message>::current_item() {
	auto msg_id = **current;
	return current_pst()->open_message(msg_id);
}

template <>
folder PSTConcreteIteratorState<vector<node_id>::iterator, folder>::current_item() {
	auto msg_id = **current;
	return current_pst()->open_folder(msg_id);
}

template <typename it, typename t>
bool PSTConcreteIteratorState<it, t>::bind_next() {
	if (!finished())
		return false;

	std::optional<std::pair<OpenFileInfo, node_id>> next = global_state.take_folder();

	if (!next)
		return false;

	auto &[next_file, next_folder_id] = *next;

	file = std::move(next_file);
	folder_id = next_folder_id;
	pst.emplace(pstsdk::pst(utils::to_wstring(file.path)));

	bind_iter();

	return true;
}

const std::optional<class pst> &PSTIteratorLocalTableFunctionState::current_pst() {
	return pst;
}

const OpenFileInfo &PSTIteratorLocalTableFunctionState::current_file() {
	return file;
}

template <typename it, typename t>
idx_t PSTConcreteIteratorState<it, t>::emit_rows(DataChunk &output) {
	idx_t rows = 0;

	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		auto item = next();

		if (!item) {
			break;
		}

		schema::into_row<t>(*this, output, *item, i);

		++rows;
	}

	return rows;
}

// PSTIteratorLocalTableFunctionState (NDB Folders)
template <>
void PSTConcreteIteratorState<pst::folder_iterator, folder>::bind_iter() {
	current = pst->folder_begin();
	end = pst->folder_end();
}

// PSTIteratorLocalTableFunctionState (table row)
template <>
void PSTConcreteIteratorState<folder::message_iterator, message>::bind_iter() {
	if (!folder_id) {
		throw InvalidInputException("PSTConcreteIteratorState for subfolder is missing its folder_id");
	}

	auto folder = pst->open_folder(*folder_id);
	current = folder.message_begin();
	end = folder.message_end();
}

// PSTIteratorLocalTableFunctionState (NDB Messages)
template <>
void PSTConcreteIteratorState<pst::message_iterator, message>::bind_iter() {
	current = pst->message_begin();
	end = pst->message_end();
}

// PSTIteratorLocalTableFunctionState (vector of messages)
template <>
void PSTConcreteIteratorState<vector<node_id>::iterator, message>::bind_iter() {
	current = message_batch->begin();
	end = message_batch->end();
}

template <>
bool PSTConcreteIteratorState<vector<node_id>::iterator, message>::bind_next() {
	auto next = global_state.take_messages(DEFAULT_STANDARD_VECTOR_SIZE);

	if (!next)
		return false;

	auto &[next_file, next_batch] = *next;

	file = std::move(next_file);
	message_batch.emplace(std::move(next_batch));
	pst.emplace(pstsdk::pst(utils::to_wstring(file.path)));

	bind_iter();

	return true;
}

template class PSTConcreteIteratorState<pst::folder_iterator, folder>;
template class PSTConcreteIteratorState<pst::message_iterator, message>;
template class PSTConcreteIteratorState<folder::message_iterator, message>;
template class PSTConcreteIteratorState<vector<node_id>::iterator, message>;
} // namespace intellekt::duckpst

#include "function_state.hpp"
#include "duckdb/common/exception.hpp"
#include "pst_schema.hpp"
#include "pstsdk/pst/message.h"
#include "table_function.hpp"
#include "utils.hpp"
#include <optional>
#include <utility>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// PSTReadGlobalTableFunctionState
PSTReadGlobalTableFunctionState::PSTReadGlobalTableFunctionState(queue<OpenFileInfo> &&files,
                                                                 queue<queue<node_id>> &&folder_queue,
                                                                 const PSTReadFunctionMode mode,
                                                                 const LogicalType &output_schema)
    : files(boost::synchronized_value<queue<OpenFileInfo>>(files)), folder_ids(folder_queue), mode(mode),
      output_schema(output_schema) {
}

idx_t PSTReadGlobalTableFunctionState::MaxThreads() const {
	return files->size();
}

std::optional<std::pair<OpenFileInfo, node_id>> PSTReadGlobalTableFunctionState::take() {
	auto sync_files = files.synchronize();
	if (sync_files->empty())
		return {};

	auto file = sync_files->front();
	auto folder_id = folder_ids.front().front();

	folder_ids.front().pop();

	if (folder_ids.front().empty() || mode == PSTReadFunctionMode::Folder) {
		sync_files->pop();
	}

	return std::make_pair(file, folder_id);
}

// PSTIteratorLocalTableFunctionState
PSTIteratorLocalTableFunctionState::PSTIteratorLocalTableFunctionState(OpenFileInfo &&file,
                                                                       std::optional<node_id> &&maybe_folder_id,
                                                                       PSTReadGlobalTableFunctionState &global_state)
    : file(file), folder_id(maybe_folder_id), global_state(global_state) {
}

template <typename it, typename t>
PSTConcreteIteratorState<it, t>::PSTConcreteIteratorState(OpenFileInfo &&file, std::optional<node_id> &&maybe_folder_id,
                                                          PSTReadGlobalTableFunctionState &global_state)
    : PSTIteratorLocalTableFunctionState(std::move(file), std::move(maybe_folder_id), global_state) {
	pst.emplace(pstsdk::pst(utils::to_wstring(file.path)));
	bind_iter();
}

template <typename it, typename t>
PSTConcreteIteratorState<it, t>::PSTConcreteIteratorState(OpenFileInfo &&file,
                                                          PSTReadGlobalTableFunctionState &global_state)
    : PSTConcreteIteratorState(std::move(file), std::nullopt, global_state) {
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

	std::optional<std::pair<OpenFileInfo, node_id>> next = global_state.take();

	if (!next)
		return false;

	OpenFileInfo next_file;
	node_id next_folder_id;

	std::tie(file, next_folder_id) = *next;

	file = std::move(next_file);
	folder_id = std::move(next_folder_id);

	pst.emplace(pstsdk::pst(utils::to_wstring(next_file.path)));
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

template <typename it, typename t>
idx_t PSTConcreteIteratorState<it, t>::emit_rows(DataChunk &output) {
	idx_t rows = 0;

	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		auto item = next();
		if (!item) {
			break;
		}

		schema::into_row<t>(output, *item, i);

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

template class PSTConcreteIteratorState<pst::folder_iterator, folder>;
template class PSTConcreteIteratorState<pst::message_iterator, message>;
template class PSTConcreteIteratorState<folder::message_iterator, message>;
} // namespace intellekt::duckpst

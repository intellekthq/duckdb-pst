#include "function_state.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/logging/logger.hpp"
#include "row_serializer.hpp"
#include "pstsdk/pst/folder.h"
#include "pstsdk/pst/message.h"
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
    : bind_data(bind_data), column_ids(std::move(column_ids)) {
	auto sync_partitions = partitions.synchronize();
	for (auto &part : bind_data.partitions) {
		sync_partitions->push(part);
	}
}

std::optional<PSTInputPartition> PSTReadGlobalTableFunctionState::take_partition() {
	auto sync_partitions = partitions.synchronize();
	if (sync_partitions->empty())
		return {};

	auto part = sync_partitions->front();

	// TODO: it would be more honest if this happened after emission
	nodes_processed += part.stats.count;

	sync_partitions->pop();
	return std::move(part);
}

idx_t PSTReadGlobalTableFunctionState::MaxThreads() const {
	return std::max<idx_t>(partitions->size(), 1);
}

// PSTIteratorLocalTableFunctionState
PSTIteratorLocalTableFunctionState::PSTIteratorLocalTableFunctionState(PSTReadGlobalTableFunctionState &global_state,
                                                                       ExecutionContext &ec)
    : global_state(global_state), ec(ec) {
	bind_partition();
}

bool PSTIteratorLocalTableFunctionState::bind_partition() {
	auto next_partition = global_state.take_partition();
	if (!partition.has_value())
		return false;
	partition.emplace(std::move(*next_partition));
	pst.emplace(pstsdk::pst(utils::to_wstring(partition->file.path)));

	return true;
}

const vector<column_t> &PSTIteratorLocalTableFunctionState::column_ids() {
	return global_state.column_ids;
}

const LogicalType &PSTIteratorLocalTableFunctionState::output_schema() {
	return duckpst::output_schema(global_state.bind_data.mode);
}

// PSTConcreteIteratorState
template <typename t>
PSTConcreteIteratorState<t>::PSTConcreteIteratorState(PSTReadGlobalTableFunctionState &global_state,
                                                      ExecutionContext &ec)
    : PSTIteratorLocalTableFunctionState(global_state, ec) {
}

template <typename t>
const bool PSTConcreteIteratorState<t>::finished() {
	return (!current) || (current == end);
}

template <typename t>
std::optional<t> PSTConcreteIteratorState<t>::next() {
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

template <>
message PSTConcreteIteratorState<message>::current_item() {
	auto msg_id = **current;
	return pst->open_message(msg_id);
}

template <>
folder PSTConcreteIteratorState<folder>::current_item() {
	auto msg_id = **current;
	return pst->open_folder(msg_id);
}

template <typename t>
bool PSTConcreteIteratorState<t>::bind_next() {
	bool bound = !finished() && this->bind_next();
	if (bound) {
		current.emplace(partition->nodes.begin());
		end.emplace(partition->nodes.end());
	}

	return bound;
}

template <typename t>
idx_t PSTConcreteIteratorState<t>::emit_rows(DataChunk &output) {
	idx_t rows = 0;

	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		auto item = next();

		if (!item) {
			break;
		}

		row_serializer::into_row<t>(*this, output, *item, i);

		++rows;
	}

	return rows;
}

template class PSTConcreteIteratorState<folder>;
template class PSTConcreteIteratorState<message>;

} // namespace intellekt::duckpst

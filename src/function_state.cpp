#include "function_state.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/logging/logger.hpp"
#include "row_serializer.hpp"
#include "pstsdk/pst/folder.h"
#include "pstsdk/pst/message.h"
#include "table_function.hpp"
#include "pstsdk_duckdb_filesystem.hpp"
#include "utils.hpp"
#include <optional>
#include <utility>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// PSTReadGlobalTableFunctionState
PSTReadGlobalState::PSTReadGlobalState(const PSTReadTableFunctionData &bind_data, vector<column_t> column_ids)
    : bind_data(bind_data), column_ids(std::move(column_ids)) {
	auto sync_partitions = partitions.synchronize();
	for (auto &part : bind_data.partitions) {
		sync_partitions->push(part);
	}
}

std::optional<PSTInputPartition> PSTReadGlobalState::take_partition() {
	auto sync_partitions = partitions.synchronize();
	if (sync_partitions->empty())
		return {};

	auto part = sync_partitions->front();

	// TODO: it would be more honest if this happened after emission
	nodes_processed += part.stats.count;

	sync_partitions->pop();
	return std::move(part);
}

idx_t PSTReadGlobalState::MaxThreads() const {
	return std::max<idx_t>(partitions->size(), 1);
}

// PSTIteratorLocalTableFunctionState
PSTReadLocalState::PSTReadLocalState(PSTReadGlobalState &global_state, ExecutionContext &ec)
    : global_state(global_state), ec(ec) {
	bind_partition();
}

bool PSTReadLocalState::bind_partition() {
	auto next_partition = global_state.take_partition();
	if (!next_partition.has_value())
		return false;

	bool skip_bind_pst = partition.has_value() && (next_partition->file.path == partition->file.path);
	partition.emplace(std::move(*next_partition));
	if (!skip_bind_pst) pst.emplace(pstsdk::pst(*partition->pst));

	return true;
}

const vector<column_t> &PSTReadLocalState::column_ids() {
	return global_state.column_ids;
}

const LogicalType &PSTReadLocalState::output_schema() {
	return duckpst::output_schema(global_state.bind_data.mode);
}

// PSTConcreteIteratorState
template <typename t>
PSTReadRowSpoolerState<t>::PSTReadRowSpoolerState(PSTReadGlobalState &global_state, ExecutionContext &ec)
    : PSTReadLocalState(global_state, ec) {
	if (partition.has_value()) {
		current.emplace(partition->nodes.begin());
		end.emplace(partition->nodes.end());
	}
}

template <typename t>
const bool PSTReadRowSpoolerState<t>::finished() {
	return (!current) || (current == end);
}

template <typename t>
std::optional<t> PSTReadRowSpoolerState<t>::next() {
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
message PSTReadRowSpoolerState<message>::current_item() {
	auto msg_id = **current;
	return pst->open_message(msg_id);
}

template <>
folder PSTReadRowSpoolerState<folder>::current_item() {
	auto msg_id = **current;
	return pst->open_folder(msg_id);
}

template <typename t>
bool PSTReadRowSpoolerState<t>::bind_next() {
	while (finished()) {
		if (!bind_partition())
			return false;
		current.emplace(partition->nodes.begin());
		end.emplace(partition->nodes.end());
	}

	return true;
}

template <typename t>
idx_t PSTReadRowSpoolerState<t>::emit_rows(DataChunk &output) {
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

template class PSTReadRowSpoolerState<folder>;
template class PSTReadRowSpoolerState<message>;

} // namespace intellekt::duckpst

#include "function_state.hpp"
#include "pst/duckdb_filesystem.hpp"
#include "pst/typed_bag.hpp"
#include "row_serializer.hpp"
#include "table_function.hpp"

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/logging/logger.hpp"

#include <optional>
#include <utility>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// PSTReadGlobalTableFunctionState
PSTReadGlobalState::PSTReadGlobalState(const PSTReadTableFunctionData &bind_data, vector<column_t> column_ids)
    : bind_data(bind_data), column_ids(std::move(column_ids)) {
	auto sync_partitions = partitions.synchronize();
	for (auto &part : bind_data.partitions.get()) {
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

PSTReadLocalState::PSTReadLocalState(PSTReadGlobalState &global_state, ExecutionContext &ec)
    : global_state(global_state), ec(ec) {
	bind_partition();
	if (partition.has_value()) {
		current.emplace(partition->nodes.begin());
		end.emplace(partition->nodes.end());
	}
}

bool PSTReadLocalState::bind_partition() {
	auto next_partition = global_state.take_partition();
	if (!next_partition.has_value())
		return false;

	bool skip_bind_pst = partition.has_value() && (next_partition->file.path == partition->file.path);
	partition.emplace(std::move(*next_partition));
	if (!skip_bind_pst)
		pst.emplace(pstsdk::pst(*partition->pst));

	return true;
}

const vector<column_t> &PSTReadLocalState::column_ids() {
	return global_state.column_ids;
}

const LogicalType &PSTReadLocalState::output_schema() {
	return duckpst::output_schema(global_state.bind_data.mode);
}

const bool PSTReadLocalState::finished() {
	return (!current) || (current == end);
}

bool PSTReadLocalState::bind_next() {
	while (finished()) {
		if (!bind_partition())
			return false;
		current.emplace(partition->nodes.begin());
		end.emplace(partition->nodes.end());
	}

	return true;
}

template <pst::MessageClass V, typename T>
PSTReadConcreteLocalState<V, T>::PSTReadConcreteLocalState(PSTReadGlobalState &global_state, ExecutionContext &ec)
    : PSTReadLocalState(global_state, ec) {
}

template <pst::MessageClass V, typename T>
std::optional<pst::TypedBag<V, T>> PSTReadConcreteLocalState<V, T>::next() {
	// If the current state is finished, keep going until we can keep binding
	while (finished() && bind_next()) {
	}

	// If we can't bind anymore and are finished, we're really finished
	if (finished())
		return {};

	pst::TypedBag<V, T> typed_bag(*partition->pst, **current);

	++(*current);
	return typed_bag;
}

template <pst::MessageClass V, typename T>
idx_t PSTReadConcreteLocalState<V, T>::emit_rows(DataChunk &output) {
	idx_t rows = 0;

	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		auto item = next();

		if (!item) {
			break;
		}

		row_serializer::into_row<pst::TypedBag<V, T>>(*this, output, *item, i);

		++rows;
	}

	return rows;
}

template class PSTReadConcreteLocalState<pst::MessageClass::Note, pstsdk::folder>;
template class PSTReadConcreteLocalState<pst::MessageClass::Note>;
template class PSTReadConcreteLocalState<pst::MessageClass::Contact>;
template class PSTReadConcreteLocalState<pst::MessageClass::Appointment>;
template class PSTReadConcreteLocalState<pst::MessageClass::StickyNote>;
template class PSTReadConcreteLocalState<pst::MessageClass::Task>;

} // namespace intellekt::duckpst

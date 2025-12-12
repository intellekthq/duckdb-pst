#include "function_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "row_serializer.hpp"
#include "pstsdk/pst/folder.h"
#include "pstsdk/pst/message.h"
#include "schema.hpp"
#include "table_function.hpp"
#include "pstsdk_duckdb_filesystem.hpp"
#include <optional>
#include <utility>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

bool apply_filter(Value &v, TableFilter &t, std::optional<ClientContext> ctx) {
	bool match = true;

	switch (t.filter_type) {
	case duckdb::TableFilterType::CONSTANT_COMPARISON:
		match = t.Cast<ConstantFilter>().Compare(v);
		break;
	case duckdb::TableFilterType::IS_NULL:
		match = v.IsNull();
		break;
	case duckdb::TableFilterType::IS_NOT_NULL:
		match = !v.IsNull();
		break;
	case duckdb::TableFilterType::CONJUNCTION_OR:
		for (auto &c : t.Cast<ConjunctionOrFilter>().child_filters) {
			match = match || apply_filter(v, *c);
		}
		break;
	case duckdb::TableFilterType::CONJUNCTION_AND:
		for (auto &c : t.Cast<ConjunctionAndFilter>().child_filters) {
			match = match && apply_filter(v, *c);
		}
		break;
	case duckdb::TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = t.Cast<StructFilter>();
		auto inner_value = StructValue::GetChildren(v)[struct_filter.child_idx];
		match = apply_filter(inner_value, *struct_filter.child_filter);
	} break;
	case duckdb::TableFilterType::IN_FILTER:
		for (auto &in_val : t.Cast<InFilter>().values) {
			match = match && (in_val == v);
		}
		break;
	case duckdb::TableFilterType::EXPRESSION_FILTER: {
		auto &expr_filter = t.Cast<ExpressionFilter>();
		if (!ctx)
			throw InvalidInputException("Client context required to evaluate this filter: %s",
			                            expr_filter.expr->ToString());
		match = expr_filter.EvaluateWithConstant(*ctx, v);
		break;
	}
	case duckdb::TableFilterType::BLOOM_FILTER:
	case duckdb::TableFilterType::DYNAMIC_FILTER:
	case duckdb::TableFilterType::OPTIONAL_FILTER:
	default:
		break;
	}

	return match;
}

// PSTReadGlobalTableFunctionState
PSTReadGlobalState::PSTReadGlobalState(const PSTReadTableFunctionData &bind_data, vector<column_t> column_ids,
                                       std::optional<unique_ptr<TableFilterSet>> filters)
    : bind_data(bind_data), column_ids(std::move(column_ids)), filters(std::move(filters)) {
	std::optional<unique_ptr<TableFilter>> partition_filter;
	std::optional<unique_ptr<TableFilter>> node_id_filter;

	if (this->filters) {
		for (auto &[col_id, f] : this->filters->get()->filters) {
			switch (this->column_ids[col_id]) {
			case schema::PST_PARTITION_INDEX:
				partition_filter.emplace(f->Copy());
				break;
			case schema::PST_ITEM_NODE_ID:
				node_id_filter.emplace(f->Copy());
				break;
			}
		}
	}

	auto sync_partitions = partitions.synchronize();
	for (auto &part : bind_data.partitions) {
		if (!filters) {
			sync_partitions->push(part);
			continue;
		}

		auto pindex = Value::UBIGINT(part.partition_index);

		if (partition_filter && !apply_filter(pindex, **partition_filter))
			continue;

		if (!node_id_filter) {
			sync_partitions->push(part);
			continue;
		}

		vector<node_id> filtered_nodes;
		for (auto &nid : part.nodes) {
			auto nid_value = Value::UINTEGER(nid);
			if (apply_filter(nid_value, **node_id_filter)) {
				filtered_nodes.push_back(nid);
			}
		}

		PartitionStatistics stats;

		stats.count = filtered_nodes.size();
		stats.count_type = CountType::COUNT_EXACT;

		sync_partitions->push(PSTInputPartition(part.partition_index, part.pst, part.file, part.mode,
		                                        std::move(filtered_nodes), std::move(stats)));
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

template <typename t>
node_id PSTReadRowSpoolerState<t>::current_node_id() {
	return **current;
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

template class PSTReadRowSpoolerState<message>;
template class PSTReadRowSpoolerState<folder>;

} // namespace intellekt::duckpst

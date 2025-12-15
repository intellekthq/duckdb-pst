#include "table_function.hpp"
#include "function_state.hpp"
#include "pst/message.hpp"
#include "schema.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include "pst/duckdb_filesystem.hpp"
#include "pstsdk/pst/pst.h"
#include "pstsdk/pst/folder.h"

#include <exception>
#include <future>
#include <limits>

namespace intellekt::duckpst {
using namespace duckdb;

PSTInputPartition::PSTInputPartition(const idx_t partition_index, const shared_ptr<pstsdk::pst> pst,
                                     const OpenFileInfo file, const PSTReadFunctionMode mode, PartitionStatistics stats,
                                     const vector<node_id> &&nodes)
    : partition_index(partition_index), pst(pst), file(file), mode(mode), stats(std::move(stats)), nodes(nodes) {
}

PSTInputPartition::PSTInputPartition(const PSTInputPartition &other_partition)
    : partition_index(other_partition.partition_index), pst(other_partition.pst), file(other_partition.file),
      mode(other_partition.mode), stats(other_partition.stats), nodes(other_partition.nodes) {};

PSTReadTableFunctionData::PSTReadTableFunctionData(ClientContext &ctx, const string &&path,
                                                   const PSTReadFunctionMode mode,
                                                   duckdb::named_parameter_map_t &named_parameters)
    : named_parameters(named_parameters), mode(mode) {
	auto &fs = FileSystem::GetFileSystem(ctx);

	if (FileSystem::HasGlob(path)) {
		files = fs.GlobFiles(path, ctx);
	} else {
		files.push_back(OpenFileInfo(path));
	}

	plan_input_partitions(ctx);
}

template <typename T>
const T PSTReadTableFunctionData::parameter_or_default(const char *parameter_name, T default_value) const {
	auto maybe_item = named_parameters.find(parameter_name);
	if (maybe_item == named_parameters.end())
		return default_value;
	auto &[_, v] = *maybe_item;
	return v.GetValue<T>();
}

const idx_t PSTReadTableFunctionData::partition_size() const {
	return std::max<idx_t>(parameter_or_default("partition_size", DEFAULT_PARTITION_SIZE), 1);
}

const idx_t PSTReadTableFunctionData::max_body_size_bytes() const {
	return parameter_or_default("max_body_size_bytes", DEFAULT_BODY_SIZE_BYTES);
}

const bool PSTReadTableFunctionData::read_attachment_body() const {
	return parameter_or_default("read_attachment_body", false);
}

const idx_t PSTReadTableFunctionData::read_limit() const {
	return parameter_or_default("read_limit", std::numeric_limits<idx_t>().max());
}

void PSTReadTableFunctionData::bind_table_function_output_schema(vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	auto schema = output_schema(mode);
	for (idx_t i = 0; i < StructType::GetChildCount(schema); ++i) {
		names.emplace_back(StructType::GetChildName(schema, i));
		return_types.emplace_back(StructType::GetChildType(schema, i));
	}
}

// TODO: this applies a filter when mode is not message
void PSTReadTableFunctionData::plan_file_partitions(ClientContext &ctx, OpenFileInfo &file, idx_t limit) {
	auto pst = make_shared_ptr<pstsdk::pst>(pst::dfile::open(ctx, file));
	vector<node_id> nodes;

	std::optional<PSTInputPartition> tail;
	if (!this->partitions->empty())
		tail.emplace(this->partitions->back());
	if (tail) {
		idx_t total = 0;
		if (tail->stats.row_start.IsValid())
			total = tail->stats.row_start.GetIndex();
		if ((total + tail->stats.count) >= limit)
			return;
	}

	if (mode == PSTReadFunctionMode::Folder) {
		for (pstsdk::pst::folder_filter_iterator it = pst->folder_node_begin(); it != pst->folder_node_end(); ++it) {
			if (nodes.size() >= limit)
				break;
			nodes.emplace_back(it->id);
		}
	} else {
		for (pstsdk::pst::message_filter_iterator it = pst->message_node_begin(); it != pst->message_node_end(); ++it) {
			auto id = it->id;

			if (mode == PSTReadFunctionMode::Message) {
				nodes.emplace_back(id);
				continue;
			}

			auto klass = pst::message_class(pst->open_message(id));
			switch (mode) {
			case PSTReadFunctionMode::Appointment:
				if (klass != pst::MessageClass::Appointment)
					continue;
				break;
			case PSTReadFunctionMode::Contact:
				if (klass != pst::MessageClass::Contact)
					continue;
				break;
			case PSTReadFunctionMode::Note:
				if (klass != pst::MessageClass::Note)
					continue;
				break;
			case PSTReadFunctionMode::StickyNote:
				if (klass != pst::MessageClass::StickyNote)
					continue;
				break;
			case PSTReadFunctionMode::Task:
				if (klass != pst::MessageClass::Task)
					continue;
				break;
			default:
				break;
			}

			nodes.emplace_back(id);
		}
	}

	auto sync_partitions = this->partitions.synchronize();
	std::optional<PSTInputPartition> sync_tail;
	if (!sync_partitions->empty())
		sync_tail.emplace(sync_partitions->back());

	idx_t total_rows = 0;
	if (sync_tail)
		total_rows = sync_tail->stats.count;

	while (!nodes.empty()) {
		vector<node_id> partition_nodes;
		PartitionStatistics stats;

		stats.row_start = total_rows;
		stats.count_type = CountType::COUNT_EXACT;

		for (idx_t i = 0; i < this->partition_size(); ++i) {
			if (i >= nodes.size())
				break;
			partition_nodes.emplace_back(nodes.back());
			nodes.pop_back();
		}

		stats.count = partition_nodes.size();
		total_rows += partition_nodes.size();

		sync_partitions->emplace_back<PSTInputPartition>(
		    {sync_partitions->size(), pst, file, mode, stats, std::move(partition_nodes)});
	}
}

void PSTReadTableFunctionData::plan_input_partitions(ClientContext &ctx) {
	if (!partitions->empty())
		return;
	auto total_rows = 0;
	auto limit = this->read_limit();

	vector<std::future<void>> plan_tasks;

	for (auto &file : files) {
		plan_tasks.emplace_back(std::async(std::launch::async, &PSTReadTableFunctionData::plan_file_partitions, this,
		                                   std::ref(ctx), std::ref(file), limit));
	}

	for (idx_t i = 0; i < files.size(); ++i) {
		try {
			plan_tasks[i].get();
		} catch (std::exception &e) {
			DUCKDB_LOG_ERROR(ctx, "Unable to read PST file (%s): %s", files[i].path, e.what());
		}
	}

	DUCKDB_LOG_INFO(ctx, "Planned %d partitions (%d files)", partitions->size(), files.size());
}

PSTReadTableFunctionData::PSTReadTableFunctionData(const PSTReadTableFunctionData &other_data) : mode(other_data.mode) {
	files = other_data.files;
	named_parameters = other_data.named_parameters;

	for (auto &part : *other_data.partitions.synchronize()) {
		this->partitions->emplace_back(PSTInputPartition(part));
	}
}

unique_ptr<FunctionData> PSTReadTableFunctionData::Copy() const {
	return make_uniq<PSTReadTableFunctionData>(*this);
}

unique_ptr<GlobalTableFunctionState> PSTReadInitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PSTReadTableFunctionData>();
	auto global_state = make_uniq<PSTReadGlobalState>(bind_data, input.column_ids);
	return global_state;
}

unique_ptr<LocalTableFunctionState> PSTReadInitLocal(ExecutionContext &ec, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global) {
	auto &bind_data = input.bind_data->Cast<PSTReadTableFunctionData>();
	auto &global_state = global->Cast<PSTReadGlobalState>();

	unique_ptr<PSTReadLocalState> local_state = nullptr;

	switch (bind_data.mode) {
	case PSTReadFunctionMode::Folder:
		local_state = make_uniq<PSTReadRowSpoolerState<folder>>(global_state, ec);
		break;
	default:
		local_state = make_uniq<PSTReadRowSpoolerState<message>>(global_state, ec);
		break;
	}

	return local_state;
}

unique_ptr<FunctionData> PSTReadBind(ClientContext &ctx, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto path = input.inputs[0].GetValue<string>();
	unique_ptr<PSTReadTableFunctionData> function_data = make_uniq<PSTReadTableFunctionData>(
	    ctx, std::move(path), FUNCTIONS.at(input.table_function.name), input.named_parameters);
	function_data->bind_table_function_output_schema(return_types, names);
	return function_data;
}

unique_ptr<NodeStatistics> PSTReadCardinality(ClientContext &ctx, const FunctionData *data) {
	auto pst_data = data->Cast<PSTReadTableFunctionData>();
	idx_t max_cardinality = 0;

	for (auto &partition : pst_data.partitions.get()) {
		max_cardinality += partition.stats.count;
	}

	auto stats = make_uniq<NodeStatistics>(max_cardinality, max_cardinality);
	return stats;
}

vector<PartitionStatistics> PSTPartitionStats(ClientContext &ctx, GetPartitionStatsInput &input) {
	if (!input.bind_data)
		return vector<PartitionStatistics>();

	auto pst_data = input.bind_data->Cast<PSTReadTableFunctionData>();

	vector<PartitionStatistics> stats;
	for (auto &part : pst_data.partitions.get()) {
		stats.push_back(part.stats);
	}

	return stats;
}

// TODO
TablePartitionInfo PSTPartitionInfo(ClientContext &ctx, TableFunctionPartitionInput &input) {
	return TablePartitionInfo::NOT_PARTITIONED;
}

double PSTReadProgress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state) {
	auto &pst_state = global_state->Cast<PSTReadGlobalState>();
	auto cardinality = PSTReadCardinality(context, bind_data)->estimated_cardinality;
	return (100.0 * pst_state.nodes_processed) / std::max<idx_t>(cardinality, 1);
}

InsertionOrderPreservingMap<string> PSTDynamicToString(duckdb::TableFunctionDynamicToStringInput &input) {
	auto &pst_data = input.bind_data->Cast<PSTReadTableFunctionData>();

	InsertionOrderPreservingMap<string> meta;

	meta.insert(make_pair("Files read", std::to_string(pst_data.files.size())));
	meta.insert(make_pair("Partitions read", std::to_string(pst_data.partitions->size())));
	meta.insert(make_pair("Partition size", std::to_string(pst_data.partition_size())));

	return meta;
}

// TODO: TIL about `MultiFileReader`
virtual_column_map_t PSTVirtualColumns(ClientContext &ctx, optional_ptr<FunctionData> bind_data) {
	DUCKDB_LOG_DEBUG(ctx, "get_virtual_columns [PSTVirtualColumns]");

	virtual_column_map_t virtual_cols;

	virtual_cols.emplace(make_pair(schema::PST_ITEM_NODE_ID, TableColumn("__node_id", schema::PST_ITEM_NODE_ID_TYPE)));
	virtual_cols.emplace(
	    make_pair(schema::PST_PARTITION_INDEX, TableColumn("__partition", schema::PST_PARTITION_INDEX_TYPE)));

	return virtual_cols;
}

vector<column_t> PSTRowIDColumns(ClientContext &ctx, optional_ptr<FunctionData> bind_data) {
	DUCKDB_LOG_DEBUG(ctx, "get_row_id_columns [PSTRowIDColumns]");
	return {schema::PST_ITEM_NODE_ID, schema::PST_PARTITION_INDEX};
}

void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output) {
	auto &local_state = input.local_state->Cast<PSTReadLocalState>();

	idx_t rows = local_state.emit_rows(output);
	output.SetCardinality(rows);
}
} // namespace intellekt::duckpst

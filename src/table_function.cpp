#include "table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_data.hpp"
#include "function_state.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include "pstsdk/pst/folder.h"
#include "utils.hpp"

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

PSTInputPartition::PSTInputPartition(const OpenFileInfo file, const PSTReadFunctionMode mode,
                                     const vector<node_id> &&nodes, const PartitionStatistics &&stats)
    : file(file), mode(mode), nodes(nodes), stats(stats) {
}

PSTReadTableFunctionData::PSTReadTableFunctionData(ClientContext &ctx, const string &&path,
                                                   const PSTReadFunctionMode mode,
                                                   duckdb::named_parameter_map_t &named_parameters)
    : mode(mode), named_parameters(named_parameters) {
	auto &fs = FileSystem::GetFileSystem(ctx);

	if (FileSystem::HasGlob(path)) {
		files = std::move(fs.GlobFiles(path, ctx));
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

void PSTReadTableFunctionData::bind_table_function_output_schema(vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	auto schema = output_schema(mode);
	for (idx_t i = 0; i < StructType::GetChildCount(schema); ++i) {
		names.emplace_back(StructType::GetChildName(schema, i));
		return_types.emplace_back(StructType::GetChildType(schema, i));
	}
}

void PSTReadTableFunctionData::plan_input_partitions(ClientContext &ctx) {
	auto total_rows = 0;

	for (auto &file : files) {
		try {
			auto pst = pstsdk::pst(utils::to_wstring(file.path));
			vector<node_id> nodes;

			// TODO
			switch (mode) {
			case PSTReadFunctionMode::Folder:
				for (pstsdk::pst::folder_filter_iterator it = pst.folder_node_begin(); it != pst.folder_node_end();
				     ++it) {
					nodes.emplace_back(it->id);
				}
				break;
			case PSTReadFunctionMode::Message:
				for (pstsdk::pst::message_filter_iterator it = pst.message_node_begin(); it != pst.message_node_end();
				     ++it) {
					nodes.emplace_back(it->id);
				}
				break;
			default:
				break;
			}

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

				partitions.emplace_back(
				    std::move(PSTInputPartition(file, mode, std::move(partition_nodes), std::move(stats))));
			}

		} catch (const std::exception &e) {
			DUCKDB_LOG_ERROR(ctx, "Unable to read PST file (%s): %s", file.path, e.what());
		}
	}

	DUCKDB_LOG_INFO(ctx, "Planned %d partitions (%d files)", partitions.size(), files.size());
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
	case PSTReadFunctionMode::Message:
		local_state = make_uniq<PSTReadRowSpoolerState<message>>(global_state, ec);
		break;
	default:
		break;
	}

	if (!local_state)
		return nullptr;
	return std::move(local_state);
}

unique_ptr<FunctionData> PSTReadBind(ClientContext &ctx, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto path = input.inputs[0].GetValue<string>();
	unique_ptr<PSTReadTableFunctionData> function_data = make_uniq<PSTReadTableFunctionData>(
	    ctx, std::move(path), FUNCTIONS.at(input.table_function.name), input.named_parameters);
	function_data->bind_table_function_output_schema(return_types, names);
	return std::move(function_data);
}

unique_ptr<NodeStatistics> PSTReadCardinality(ClientContext &ctx, const FunctionData *data) {
	auto pst_data = data->Cast<PSTReadTableFunctionData>();
	idx_t max_cardinality = 0;

	for (auto &partition : pst_data.partitions) {
		max_cardinality += partition.stats.count;
	}

	auto stats = make_uniq<NodeStatistics>(max_cardinality, max_cardinality);
	return std::move(stats);
}

vector<PartitionStatistics> PSTPartitionStats(ClientContext &ctx, GetPartitionStatsInput &input) {
	if (!input.bind_data)
		return vector<PartitionStatistics>();
	auto pst_data = input.bind_data->Cast<PSTReadTableFunctionData>();

	vector<PartitionStatistics> stats;
	for (auto &part : pst_data.partitions) {
		stats.push_back(part.stats);
	}

	return std::move(stats);
}

// TODO:
TablePartitionInfo PSTPartitionInfo(ClientContext &ctx, TableFunctionPartitionInput &input) {
	return TablePartitionInfo::NOT_PARTITIONED;
}

double PSTReadProgress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state) {
	auto &pst_state = global_state->Cast<PSTReadGlobalState>();
	auto &pst_data = bind_data->Cast<PSTReadTableFunctionData>();
	auto cardinality = PSTReadCardinality(context, bind_data)->estimated_cardinality;
	return (100.0 * pst_state.nodes_processed) / std::max<idx_t>(cardinality, 1);
}

InsertionOrderPreservingMap<string> PSTDynamicToString(duckdb::TableFunctionDynamicToStringInput &input) {
	auto &pst_data = input.bind_data->Cast<PSTReadTableFunctionData>();

	InsertionOrderPreservingMap<string> meta;

	meta.insert(make_pair("Files read", std::to_string(pst_data.files.size())));
	meta.insert(make_pair("Partitions read", std::to_string(pst_data.partitions.size())));
	meta.insert(make_pair("Partition size", std::to_string(pst_data.partition_size())));

	return std::move(meta);
}

void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<PSTReadTableFunctionData>();
	auto &local_state = input.local_state->Cast<PSTReadLocalState>();

	idx_t rows = local_state.emit_rows(output);
	output.SetCardinality(rows);
}
} // namespace intellekt::duckpst

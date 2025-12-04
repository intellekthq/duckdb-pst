#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "schema.hpp"
#include <pstsdk/pst.h>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/range/combine.hpp>
#include <boost/thread/synchronized_value.hpp>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

enum PSTReadFunctionMode { Folder, Message, NUM_SHAPES };

inline const LogicalType &output_schema(const PSTReadFunctionMode &mode) {
	switch (mode) {
	case PSTReadFunctionMode::Folder:
		return schema::FOLDER_SCHEMA;
	case PSTReadFunctionMode::Message:
		return schema::MESSAGE_SCHEMA;
	default:
		throw InvalidInputException("Unknown read function mode. Please report this bug on GitHub.");
	}
}

static const map<string, PSTReadFunctionMode> FUNCTIONS = {
    {"read_pst_folders", Folder},
    {"read_pst_messages", Message},
};

/**
 * A PST read as expressed by node IDs in a file
 */
struct PSTInputPartition {
	const OpenFileInfo file;
	const PSTReadFunctionMode mode;
	const PartitionStatistics stats;
	vector<node_id> nodes;

	PSTInputPartition(const OpenFileInfo file, const PSTReadFunctionMode mode, const vector<node_id> &&nodes,
	                  const PartitionStatistics &&stats);
};

struct PSTReadTableFunctionData : public TableFunctionData {
	vector<OpenFileInfo> files;
	vector<PSTInputPartition> partitions;

public:
	const PSTReadFunctionMode mode;

	// Make a `TableFunctionData` from a path, context, and schema
	PSTReadTableFunctionData(const string &&path, ClientContext &ctx, const PSTReadFunctionMode mode);

	// Bind the column names and return types from the input schema
	void bind_table_function_output_schema(vector<LogicalType> &return_types, vector<string> &names);

	// Walk the PST files and populate the partition queue
	void plan_input_partitions(ClientContext &ctx);
};

unique_ptr<FunctionData> PSTReadBind(ClientContext &ctx, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> PSTReadInitGlobal(ClientContext &ctx, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> PSTReadInitLocal(ExecutionContext &ec, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global);

unique_ptr<NodeStatistics> PSTReadCardinality(ClientContext &ctx, const FunctionData *data);

vector<PartitionStatistics> PSTPartitionStats(ClientContext &ctx, GetPartitionStatsInput &input);

TablePartitionInfo PSTPartitionInfo(ClientContext &ctx, TableFunctionPartitionInput &input);

double PSTReadProgress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state);

void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output);
} // namespace intellekt::duckpst

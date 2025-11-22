#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include <pstsdk/pst.h>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/range/combine.hpp>
#include <boost/thread/synchronized_value.hpp>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

enum PSTReadFunctionMode { Folder, Message, NUM_SHAPES };

static const map<string, PSTReadFunctionMode> FNAME_TO_ENUM = {
    {"read_pst_folders", Folder},
    {"read_pst_messages", Message},
};

struct PSTReadTableFunctionData : public TableFunctionData {
	vector<OpenFileInfo> files;
	vector<vector<pstsdk::node_id>> pst_folders;

public:
	const PSTReadFunctionMode mode;
	const LogicalType &output_schema;

	// Make a `TableFunctionData` from a path, context, and schema
	PSTReadTableFunctionData(const string &&path, ClientContext &ctx, const PSTReadFunctionMode mode);

	// Bind the column names and return types from the input schema
	void bind_table_function_output_schema(vector<LogicalType> &return_types, vector<string> &names);
};

unique_ptr<FunctionData> PSTReadBind(ClientContext &ctx, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> PSTReadInitGlobal(ClientContext &ctx, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> PSTReadInitLocal(ExecutionContext &ec, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global);

unique_ptr<NodeStatistics> PSTReadCardinality(ClientContext &ctx, const FunctionData *data);

double PSTReadProgress(ClientContext &context, const FunctionData *bind_data,
                                               const GlobalTableFunctionState *global_state);

void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output);
} // namespace intellekt::duckpst

#pragma once

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"
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

static constexpr idx_t DEFAULT_PARTITION_SIZE = DEFAULT_STANDARD_VECTOR_SIZE * 2;
static constexpr idx_t DEFAULT_BODY_SIZE_BYTES = 1000000;

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

static const named_parameter_type_map_t NAMED_PARAMETERS = {{"max_body_size_bytes", LogicalType::UBIGINT},
                                                            {"partition_size", LogicalType::UBIGINT},
                                                            {"read_attachment_body", LogicalType::BOOLEAN},
                                                            {"read_limit", LogicalType::UBIGINT}};

/**
 * A PST read as expressed by node IDs in a file
 */
struct PSTInputPartition {
	const shared_ptr<pstsdk::pst> pst;
	const OpenFileInfo file;
	const PSTReadFunctionMode mode;
	const PartitionStatistics stats;
	vector<node_id> nodes;

	PSTInputPartition(const shared_ptr<pstsdk::pst> pst, const OpenFileInfo file, const PSTReadFunctionMode mode,
	                  const vector<node_id> &&nodes, const PartitionStatistics &&stats);
};

struct PSTReadTableFunctionData : public TableFunctionData {
	vector<OpenFileInfo> files;
	vector<PSTInputPartition> partitions;

	duckdb::named_parameter_map_t named_parameters;

public:
	const PSTReadFunctionMode mode;

	/**
	 * @brief Construct a new PSTReadTableFunctionData object
	 *
	 * @param path A globbable path to use with DuckDB FileSystem
	 * @param ctx ClientContext
	 * @param mode Function read mode
	 */
	PSTReadTableFunctionData(ClientContext &ctx, const string &&path, const PSTReadFunctionMode mode,
	                         duckdb::named_parameter_map_t &named_parameters);

	// Parameters
	const idx_t partition_size() const;
	const idx_t max_body_size_bytes() const;
	const bool read_attachment_body() const;
	const idx_t read_limit() const;

	/**
	 * @brief Bind table function output schema based on read mode
	 *
	 * @param return_types Positionally ordered return types
	 * @param names Positionally ordered column names
	 */
	void bind_table_function_output_schema(vector<LogicalType> &return_types, vector<string> &names);

	/**
	 * @brief Mount PSTs and bucket NDB nodes against the default DuckDB vector size
	 *
	 * @param ctx
	 */
	void plan_input_partitions(ClientContext &ctx);

private:
	template <typename T>
	const T parameter_or_default(const char *parameter_name, T default_value) const;
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

InsertionOrderPreservingMap<string> PSTDynamicToString(duckdb::TableFunctionDynamicToStringInput &);

void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output);
} // namespace intellekt::duckpst

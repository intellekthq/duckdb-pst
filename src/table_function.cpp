#include "table_function.hpp"
#include "duckdb/common/exception.hpp"
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

#include "schema.hpp"
#include "pstsdk/pst/folder.h"
#include "utils.hpp"

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// Instantiates PST read table function data:
// - Bind schema based on called table function name
// - Glob over provided path and enumerate PSTs + their folder IDs
PSTReadTableFunctionData::PSTReadTableFunctionData(const string &&path, ClientContext &ctx,
                                                   const PSTReadFunctionMode mode)
    : mode(mode), output_schema(*[&]() {
	      switch (mode) {
	      case PSTReadFunctionMode::Folder:
		      return &schema::FOLDER_SCHEMA;
		      break;
	      case PSTReadFunctionMode::Message:
		      return &schema::MESSAGE_SCHEMA;
		      break;
	      default:
		      throw InvalidInputException("Unknown read function mode. Please report this bug on GitHub.");
	      }
      }()) {
	auto &fs = FileSystem::GetFileSystem(ctx);

	if (FileSystem::HasGlob(path)) {
		files = std::move(fs.GlobFiles(path, ctx));
	} else {
		files.push_back(OpenFileInfo(path));
	}

	for (auto &file : files) {
		vector<node_id> folders;

		try {
			auto pst = pstsdk::pst(utils::to_wstring(file.path));
			for (auto it = pst.folder_begin(); it != pst.folder_end(); ++it) {
				folders.emplace_back(it->get_id());
			}
		} catch (const std::exception &e) {
			DUCKDB_LOG_ERROR(ctx, "Unable to read PST file (%s): %s", file.path, e.what());
		}

		this->pst_folders.push_back(std::move(folders));
	}
}

void PSTReadTableFunctionData::bind_table_function_output_schema(vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	for (idx_t i = 0; i < StructType::GetChildCount(output_schema); ++i) {
		names.emplace_back(StructType::GetChildName(output_schema, i));
		return_types.emplace_back(StructType::GetChildType(output_schema, i));
	}
}

// Load enumerated function state data into queues for spooler state
unique_ptr<GlobalTableFunctionState> PSTReadInitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PSTReadTableFunctionData>();

	auto global_state = make_uniq<PSTReadGlobalTableFunctionState>(bind_data, input.column_ids);

	return global_state;
}

// Local function states can spool different files, or concurrently spool different folders from the same file
unique_ptr<LocalTableFunctionState> PSTReadInitLocal(ExecutionContext &ec, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global) {
	auto &bind_data = input.bind_data->Cast<PSTReadTableFunctionData>();
	auto &global_state = global->Cast<PSTReadGlobalTableFunctionState>();

	unique_ptr<PSTIteratorLocalTableFunctionState> local_state = nullptr;

	switch (bind_data.mode) {
	case PSTReadFunctionMode::Folder:
		local_state = make_uniq<PSTConcreteIteratorState<pst::folder_iterator, folder>>(global_state, ec);
		break;
	case PSTReadFunctionMode::Message:
		local_state = make_uniq<PSTConcreteIteratorState<vector<node_id>::iterator, message>>(global_state, ec);
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
	unique_ptr<PSTReadTableFunctionData> function_data =
	    make_uniq<PSTReadTableFunctionData>(std::move(path), ctx, FUNCTIONS.at(input.table_function.name));
	function_data->bind_table_function_output_schema(return_types, names);
	return std::move(function_data);
}

// Mount files from function data and count the number of folders/messages for cardinality estimates.
// Folders: we already enumerated them when making function data state
// Messages: we read the pbag attribute with a count instead of walking the iterator
unique_ptr<NodeStatistics> PSTReadCardinality(ClientContext &ctx, const FunctionData *data) {
	auto pst_data = data->Cast<PSTReadTableFunctionData>();
	idx_t estimated_cardinality = 0;

	// TODO: need to do a pass of "how many times do we open the file"
	for (auto p : boost::combine(pst_data.files, pst_data.pst_folders)) {
		auto &[file, folders] = p;

		switch (pst_data.mode) {
		case PSTReadFunctionMode::Folder: {
			estimated_cardinality += folders.size();
			break;
		}
		case PSTReadFunctionMode::Message: {
			auto pst = pstsdk::pst(utils::to_wstring(file.path));

			for (auto folder_id : folders) {
				auto folder = pst.open_folder(folder_id);
				estimated_cardinality += folder.get_message_count();
			}

			break;
		}
		default:
			break;
		}
	}

	auto stats = make_uniq<NodeStatistics>(estimated_cardinality);
	return std::move(stats);
}

double PSTReadProgress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state) {
	auto &pst_state = global_state->Cast<PSTReadGlobalTableFunctionState>();
	return pst_state.progress();
}

void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<PSTReadTableFunctionData>();
	auto &local_state = input.local_state->Cast<PSTIteratorLocalTableFunctionState>();

	idx_t rows = local_state.emit_rows(output);
	output.SetCardinality(rows);
}
} // namespace intellekt::duckpst

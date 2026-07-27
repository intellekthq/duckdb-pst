#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "schema.hpp"

#include <boost/thread/synchronized_value.hpp>
#include <pstsdk/pst.h>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

/**
 * @brief Which SDK call a delete function issues
 *
 * Scoped, unlike PSTReadFunctionMode, which already owns Message and Folder in
 * this namespace
 */
enum class PSTDeleteFunctionMode { Message, Folder, Attachment, FreeSpace };

/**
 * @brief Terminal state of one target, ordered to match DELETE_STATUS_ENUM
 */
enum class PSTDeleteStatus { Previewed, Deleted, Failed };

inline const map<string, PSTDeleteFunctionMode> DELETE_FUNCTIONS = {
    {"delete_pst_messages", PSTDeleteFunctionMode::Message},
    {"delete_pst_folders", PSTDeleteFunctionMode::Folder},
    {"delete_pst_attachments", PSTDeleteFunctionMode::Attachment},
    {"wipe_pst_free_space", PSTDeleteFunctionMode::FreeSpace}};

inline const named_parameter_type_map_t DELETE_NAMED_PARAMETERS = {
    {"really", LogicalType::BOOLEAN}};

/**
 * @brief One node the caller asked to delete
 */
struct PSTDeleteTarget {
  node_id node;
  // The owning message for an attachment, zero otherwise
  node_id owner;
};

/**
 * @brief One row of a delete function's output
 */
struct PSTDeleteResult {
  string path;
  node_id node;
  node_id owner;
  idx_t bytes_wiped;
  PSTDeleteStatus status;
  string error;
};

/**
 * @brief Bind data shared by the delete functions and the free space wipe
 */
struct PSTDeleteTableFunctionData : public TableFunctionData {
  duckdb::named_parameter_map_t named_parameters;

public:
  const PSTDeleteFunctionMode mode;
  // Carried so error messages can name the function the caller actually typed
  const string function_name;

  // Only populated for FreeSpace, which takes a globbable path
  vector<OpenFileInfo> files;

  PSTDeleteTableFunctionData(const PSTDeleteFunctionMode mode,
                             const string &function_name,
                             duckdb::named_parameter_map_t &named_parameters);

  /**
   * @brief Whether the caller asked for the deletion to actually happen
   *
   * @return true Mutate the store
   * @return false Resolve every target and report what would happen
   */
  const bool really() const;

  /**
   * @brief Glob the path a free space wipe was given
   *
   * @param ctx
   * @param path Path or glob
   */
  void bind_files(ClientContext &ctx, const string &path);

  /**
   * @brief Bind this mode's output schema
   *
   * @param return_types Positionally ordered return types
   * @param names Positionally ordered column names
   */
  void bind_table_function_output_schema(vector<LogicalType> &return_types,
                                         vector<string> &names);

  /**
   * @brief Hold the nested input table to this mode's column contract,
   *        rewriting input_table_types so DuckDB inserts the casts
   *
   * @param input Bind input, whose input_table_types is rewritten in place
   */
  void bind_input_table_schema(ClientContext &ctx,
                               TableFunctionBindInput &input);

  unique_ptr<FunctionData> Copy() const override;

private:
  template <typename T>
  const T parameter_or_default(const char *parameter_name,
                               T default_value) const;
};

/**
 * Deletion targets bucketed per file, so each store is opened writable exactly
 * once and drained in one pass once the source scan has finished.
 */
class PSTDeleteGlobalState : public GlobalTableFunctionState {
  struct Buffer {
    // Insertion ordered, because input rows arrive interleaved across files
    vector<string> paths;
    unordered_map<string, idx_t> path_index;
    vector<vector<PSTDeleteTarget>> targets;
    // How far into each file's targets the last drain got. A UNION ALL input
    // is two pipelines, and DuckDB finalizes once per pipeline, so a drain
    // that latched would silently drop everything the second one buffered
    vector<idx_t> drained_upto;

    vector<PSTDeleteResult> results;
    idx_t emitted = 0;
  };

  boost::synchronized_value<Buffer> buffer;

  /**
   * @brief Apply one file's targets, recording a result for each
   *
   * @param ctx
   * @param path The store to edit
   * @param targets Nodes to delete
   * @param from First target not yet drained
   * @param results Where to append this file's results
   */
  void drain_file(ClientContext &ctx, const string &path,
                  const vector<PSTDeleteTarget> &targets, idx_t from,
                  vector<PSTDeleteResult> &results);

public:
  PSTDeleteGlobalState(const PSTDeleteTableFunctionData &bind_data);
  const PSTDeleteTableFunctionData &bind_data;

  /**
   * @brief Bucket one target under its file, preserving arrival order
   *
   * @param path PST path as the caller gave it
   * @param target Node to delete
   */
  void buffer_target(const string &path, const PSTDeleteTarget target);

  /**
   * @brief Open each buffered file and apply the targets that arrived since
   *        the last call, so a second finalize picks up a second pipeline's
   *        rows without redeleting the first one's
   *
   * @param ctx
   */
  void drain(ClientContext &ctx);

  /**
   * @brief Spool buffered results into a chunk
   *
   * @param output Current data chunk
   * @return idx_t Number of rows written
   */
  idx_t emit_rows(DataChunk &output);

  /**
   * @brief Are all results emitted?
   */
  const bool finished();

  // One writer per store, so one thread for the whole operator
  idx_t MaxThreads() const override { return 1; }
};

/**
 * @brief Bind a delete function, checking the gate and the input table
 */
unique_ptr<FunctionData> PSTDeleteBind(ClientContext &ctx,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names);

/**
 * @brief Bind the free space wipe, which takes a path rather than a table
 */
unique_ptr<FunctionData> PSTWipeBind(ClientContext &ctx,
                                     TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types,
                                     vector<string> &names);

unique_ptr<GlobalTableFunctionState>
PSTDeleteInitGlobal(ClientContext &ctx, TableFunctionInitInput &input);

/**
 * @brief Buffer the input rows. Emits nothing, which is what makes the finalize
 *        pass a barrier against the scan that produced them.
 */
OperatorResultType PSTDeleteFunction(ExecutionContext &ec,
                                     TableFunctionInput &data, DataChunk &input,
                                     DataChunk &output);

/**
 * @brief Apply every buffered target, then spool the results
 */
OperatorFinalizeResultType PSTDeleteFinalFunction(ExecutionContext &ec,
                                                  TableFunctionInput &data,
                                                  DataChunk &output);

/**
 * @brief Wipe the free space of every file the path matched
 */
void PSTWipeFunction(ClientContext &ctx, TableFunctionInput &data,
                     DataChunk &output);

} // namespace intellekt::duckpst

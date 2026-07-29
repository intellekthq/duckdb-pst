#include "delete_function.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include "pst/duckdb_filesystem.hpp"
#include "schema.hpp"

#include <pstsdk/pst.h>

namespace intellekt::duckpst {
using namespace duckdb;

// The columns a nested read has to project, per mode. Position is the only
// contract available: DuckDB has no way to bind a table parameter by name
static const vector<LogicalType> NODE_INPUT_TYPES = {LogicalType::VARCHAR,
                                                     LogicalType::UINTEGER};
static const vector<string> NODE_INPUT_NAMES = {"pst_path", "node_id"};

static const vector<LogicalType> ATTACHMENT_INPUT_TYPES = {
    LogicalType::VARCHAR, LogicalType::UINTEGER, LogicalType::UINTEGER};
static const vector<string> ATTACHMENT_INPUT_NAMES = {
    "pst_path", "message_node_id", "attachment_node_id"};

/**
 * @brief Whether a path names something this extension must not edit in place
 *
 * FileSystem::IsRemoteFile is a case sensitive prefix list, so it misses
 * S3:// and anything a host registered itself. Any scheme but file:// is
 * refused instead, which fails closed on filesystems we have never heard of.
 *
 * @param path Path as the caller gave it
 */
static bool remote_path(const string &path) {
  const auto scheme_end = path.find("://");
  if (scheme_end == string::npos)
    return false;

  return !StringUtil::CIEquals(path.substr(0, scheme_end), "file");
}

// Enough of the input table to recognise, without pasting 27 column names
static const idx_t ERROR_COLUMN_LIMIT = 6;

/**
 * @brief Render an input table's columns for an error message
 *
 * @param names The child's column names
 */
static string column_list(const vector<string> &names) {
  if (names.size() <= ERROR_COLUMN_LIMIT)
    return StringUtil::Join(names, ", ");

  vector<string> head(names.begin(), names.begin() + ERROR_COLUMN_LIMIT);
  return StringUtil::Format("%s, ... (%llu more)", StringUtil::Join(head, ", "),
                            names.size() - ERROR_COLUMN_LIMIT);
}

/**
 * @brief Whether every column the mode needs is present by name
 *
 * @param names The child's column names
 * @param want The names this mode's contract calls for
 */
static bool has_columns(const vector<string> &names,
                        const vector<string> &want) {
  for (auto &needed : want) {
    bool found = false;
    for (auto &name : names)
      found = found || StringUtil::CIEquals(name, needed);

    if (!found)
      return false;
  }

  return true;
}

/**
 * @brief Whether the input's types are this mode's contract, reversed
 *
 * @param got The child's column types
 * @param want The types this mode's contract calls for
 */
static bool reversed_columns(const vector<LogicalType> &got,
                             const vector<LogicalType> &want) {
  if (got.size() != want.size())
    return false;

  for (idx_t i = 0; i < want.size(); i++)
    if (got[i] != want[want.size() - 1 - i])
      return false;

  return true;
}

// PSTDeleteTableFunctionData

PSTDeleteTableFunctionData::PSTDeleteTableFunctionData(
    const PSTDeleteFunctionMode mode, const string &function_name,
    duckdb::named_parameter_map_t &named_parameters)
    : named_parameters(named_parameters), mode(mode),
      function_name(function_name) {}

template <typename T>
const T
PSTDeleteTableFunctionData::parameter_or_default(const char *parameter_name,
                                                 T default_value) const {
  auto maybe_item = named_parameters.find(parameter_name);
  if (maybe_item == named_parameters.end())
    return default_value;
  auto &[_, v] = *maybe_item;
  // GetValue on a NULL raises an INTERNAL error, which invalidates the whole
  // database handle. really := NULL is just not a request to delete
  if (v.IsNull())
    return default_value;
  return v.GetValue<T>();
}

const bool PSTDeleteTableFunctionData::really() const {
  return parameter_or_default("really", false);
}

void PSTDeleteTableFunctionData::bind_files(ClientContext &ctx,
                                            const string &path) {
  auto &fs = FileSystem::GetFileSystem(ctx);

  // Checked here rather than on the way through: a remote glob that matches
  // nothing looks exactly like a wipe with no work to do
  if (remote_path(path))
    throw InvalidInputException(
        "cannot wipe '%s'. Wiping edits the PST in place, which only works on "
        "a local file. Copy it to local disk, wipe it there, then upload the "
        "result",
        path);

  if (FileSystem::HasGlob(path)) {
    files = fs.GlobFiles(path);
  } else {
    files.push_back(OpenFileInfo(path));
  }
}

void PSTDeleteTableFunctionData::bind_table_function_output_schema(
    vector<LogicalType> &return_types, vector<string> &names) {
  LogicalType schema;

  switch (mode) {
  case PSTDeleteFunctionMode::Attachment:
    schema = schema::DELETE_ATTACHMENT_SCHEMA;
    break;
  case PSTDeleteFunctionMode::FreeSpace:
    schema = schema::WIPE_SCHEMA;
    break;
  default:
    schema = schema::DELETE_NODE_SCHEMA;
    break;
  }

  for (auto &child : StructType::GetChildTypes(schema)) {
    names.push_back(child.first);
    return_types.push_back(child.second);
  }
}

void PSTDeleteTableFunctionData::bind_input_table_schema(
    ClientContext &ctx, TableFunctionBindInput &input) {
  const bool attachments = mode == PSTDeleteFunctionMode::Attachment;
  auto &want = attachments ? ATTACHMENT_INPUT_TYPES : NODE_INPUT_TYPES;
  auto &want_names = attachments ? ATTACHMENT_INPUT_NAMES : NODE_INPUT_NAMES;

  string signature;
  for (idx_t i = 0; i < want.size(); i++)
    signature += (i ? ", " : "") + want_names[i] + " " + want[i].ToString();

  // A SELECT * here would bind every column of the nested read and then throw
  // the bodies and attachment blobs away, so refuse it rather than do it
  if (input.input_table_types.size() != want.size()) {
    string hint =
        StringUtil::Format("The input table must be (%s).", signature);

    // The columns are usually right there, so say what to wrap rather than
    // making the caller work it out from a column count
    if (has_columns(input.input_table_names, want_names))
      hint = StringUtil::Format(
          "Those columns are in the input already, so project just them: "
          "SELECT %s FROM ...",
          StringUtil::Join(want_names, ", "));

    throw InvalidInputException(
        "%s takes an input table of %llu columns (%s), but received %llu: %s. "
        "%s",
        function_name, want.size(), signature, input.input_table_types.size(),
        column_list(input.input_table_names), hint);
  }

  for (idx_t i = 0; i < want.size(); i++) {
    if (input.input_table_types[i] == want[i])
      continue;

    if (CastFunctionSet::ImplicitCastCost(ctx, input.input_table_types[i],
                                          want[i]) >= 0)
      continue;

    // Swapping the columns is the easiest mistake to make here, and both
    // orders have the right arity, so name it rather than report a bad cast
    if (reversed_columns(input.input_table_types, want))
      throw InvalidInputException(
          "%s received its input columns in the wrong order. The input table "
          "must be (%s)",
          function_name, signature);

    throw InvalidInputException(
        "%s column %llu (%s) is %s, which cannot be read as %s. The input "
        "table must be (%s)",
        function_name, i + 1, want_names[i],
        input.input_table_types[i].ToString(), want[i].ToString(), signature);
  }

  // Rewriting these makes DuckDB insert the casts, so an INTEGER literal or a
  // BIGINT node id still binds
  input.input_table_types = want;
}

unique_ptr<FunctionData> PSTDeleteTableFunctionData::Copy() const {
  auto copy = make_uniq<PSTDeleteTableFunctionData>(*this);
  return std::move(copy);
}

// Binds

/**
 * @brief The gate is checked only for a real deletion. A preview writes
 *        nothing, and making it discoverable is the point of having one.
 */
static void check_gate(ClientContext &ctx, const string &function_name) {
  Value allow_delete;
  if (ctx.TryGetCurrentSetting("pst_allow_delete", allow_delete) &&
      !allow_delete.IsNull() && allow_delete.GetValue<bool>())
    return;

  throw InvalidInputException(
      "%s is disabled. It edits the PST in place and cannot be undone, so it "
      "needs SET pst_allow_delete = true first. Drop really := true to preview "
      "what this would do, which needs no setting and writes nothing.",
      function_name);
}

unique_ptr<FunctionData> PSTDeleteBind(ClientContext &ctx,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names) {
  DUCKDB_LOG_DEBUG(ctx, "bind [PSTDeleteBind]");

  auto function_data = make_uniq<PSTDeleteTableFunctionData>(
      DELETE_FUNCTIONS.at(input.table_function.name), input.table_function.name,
      input.named_parameters);

  if (function_data->really())
    check_gate(ctx, input.table_function.name);

  function_data->bind_input_table_schema(ctx, input);
  function_data->bind_table_function_output_schema(return_types, names);
  return std::move(function_data);
}

unique_ptr<FunctionData> PSTWipeBind(ClientContext &ctx,
                                     TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types,
                                     vector<string> &names) {
  DUCKDB_LOG_DEBUG(ctx, "bind [PSTWipeBind]");

  auto function_data = make_uniq<PSTDeleteTableFunctionData>(
      PSTDeleteFunctionMode::FreeSpace, input.table_function.name,
      input.named_parameters);

  if (function_data->really())
    check_gate(ctx, input.table_function.name);

  auto path = input.inputs[0].GetValue<string>();
  function_data->bind_files(ctx, path);
  function_data->bind_table_function_output_schema(return_types, names);
  return std::move(function_data);
}

// PSTDeleteGlobalState

PSTDeleteGlobalState::PSTDeleteGlobalState(
    const PSTDeleteTableFunctionData &bind_data)
    : bind_data(bind_data) {
  // A wipe has no input table, so its files are its targets
  for (auto &file : bind_data.files)
    buffer_target(file.path, PSTDeleteTarget{0, 0});
}

void PSTDeleteGlobalState::buffer_target(const string &path,
                                         const PSTDeleteTarget target) {
  auto sync_buffer = buffer.synchronize();

  auto maybe_index = sync_buffer->path_index.find(path);
  if (maybe_index == sync_buffer->path_index.end()) {
    sync_buffer->path_index[path] = sync_buffer->paths.size();
    sync_buffer->paths.push_back(path);
    sync_buffer->targets.push_back({});
    sync_buffer->drained_upto.push_back(0);
    maybe_index = sync_buffer->path_index.find(path);
  }

  sync_buffer->targets[maybe_index->second].push_back(target);
}

void PSTDeleteGlobalState::buffer_failure(const string &path,
                                          const PSTDeleteTarget target,
                                          const string &error) {
  auto sync_buffer = buffer.synchronize();
  sync_buffer->results.push_back(
      {path, target.node, target.owner, 0, PSTDeleteStatus::Failed, error});
}

/**
 * @brief Name a node type, so an error can say what the caller actually passed
 *
 * @param type From get_nid_type
 */
static const char *nid_type_name(pstsdk::nid_type type) {
  switch (type) {
  case pstsdk::nid_type_folder:
    return "folder";
  case pstsdk::nid_type_search_folder:
    return "search folder";
  case pstsdk::nid_type_message:
    return "message";
  case pstsdk::nid_type_associated_message:
    return "associated message";
  case pstsdk::nid_type_attachment:
    return "attachment";
  case pstsdk::nid_type_internal:
    return "store internal node";
  default:
    return "node the delete functions do not handle";
  }
}

/**
 * @brief The function that would have taken this node, when there is one
 *
 * @param type From get_nid_type
 */
static const char *suggested_function(pstsdk::nid_type type) {
  switch (type) {
  case pstsdk::nid_type_folder:
  case pstsdk::nid_type_search_folder:
    return " Use delete_pst_folders.";
  case pstsdk::nid_type_message:
  case pstsdk::nid_type_associated_message:
    return " Use delete_pst_messages.";
  case pstsdk::nid_type_attachment:
    return " Use delete_pst_attachments, which also needs the owning message.";
  default:
    return "";
  }
}

/**
 * @brief Refuse a node the mode has no business deleting
 *
 * pstsdk's delete calls take a raw nid and do no type check, so without this
 * delete_pst_messages would take the store's root folder and report DELETED.
 *
 * @param mode Which delete is running
 * @param nid The node the caller asked for
 */
static void check_deletable(PSTDeleteFunctionMode mode, node_id nid) {
  if (nid == pstsdk::nid_message_store || nid == pstsdk::nid_root_folder)
    throw InvalidInputException(
        "refusing to delete node %u: it is the store's root, and without it no "
        "client can open the file",
        nid);

  const pstsdk::nid_type type = pstsdk::get_nid_type(nid);

  if (mode == PSTDeleteFunctionMode::Message &&
      type != pstsdk::nid_type_message &&
      type != pstsdk::nid_type_associated_message)
    throw InvalidInputException("node %u is a %s, not a message.%s", nid,
                                nid_type_name(type), suggested_function(type));

  if (mode == PSTDeleteFunctionMode::Folder &&
      type != pstsdk::nid_type_folder && type != pstsdk::nid_type_search_folder)
    throw InvalidInputException("node %u is a %s, not a folder.%s", nid,
                                nid_type_name(type), suggested_function(type));
}

void PSTDeleteGlobalState::drain_file(ClientContext &ctx, const string &path,
                                      const vector<PSTDeleteTarget> &targets,
                                      idx_t from,
                                      vector<PSTDeleteResult> &results) {
  const bool really = bind_data.really();
  auto file = OpenFileInfo(path);
  const idx_t first_result = results.size();

  std::shared_ptr<pst::dfile> writable;
  pstsdk::shared_db_ptr db;

  try {
    if (really) {
      writable = pst::dfile::open_writable(ctx, file);
      db = pstsdk::open_database(writable);
    } else {
      db = pstsdk::open_database(pst::dfile::open(ctx, file));
    }
  } catch (std::exception &e) {
    const string message = ErrorData(e).RawMessage();
    DUCKDB_LOG_ERROR(ctx, "Unable to open PST file (%s): %s", path, message);
    for (idx_t t = from; t < targets.size(); t++)
      results.push_back({path, targets[t].node, targets[t].owner, 0,
                         PSTDeleteStatus::Failed, message});
    return;
  }

  // A store that reports corruption partway through is not safe to keep
  // writing to, so the rest of its targets are abandoned with the same error
  string poisoned;

  for (idx_t t = from; t < targets.size(); t++) {
    auto &target = targets[t];

    if (!poisoned.empty()) {
      results.push_back({path, target.node, target.owner, 0,
                         PSTDeleteStatus::Failed, poisoned});
      continue;
    }

    idx_t bytes_wiped = 0;

    try {
      if (bind_data.mode != PSTDeleteFunctionMode::FreeSpace)
        check_deletable(bind_data.mode, target.node);

      switch (bind_data.mode) {
      case PSTDeleteFunctionMode::Message:
        if (really)
          pstsdk::delete_message(db, target.node);
        else
          db->lookup_node_info(target.node);
        break;
      case PSTDeleteFunctionMode::Folder:
        if (really)
          pstsdk::delete_folder(db, target.node);
        else
          db->lookup_node_info(target.node);
        break;
      case PSTDeleteFunctionMode::Attachment:
        if (really)
          pstsdk::delete_attachment(db, target.owner, target.node);
        else
          db->lookup_node(target.owner).lookup(target.node);
        break;
      case PSTDeleteFunctionMode::FreeSpace:
        if (really)
          bytes_wiped = pstsdk::wipe_free_space(db);
        else
          db->read_nbt_root();
        break;
      }
    } catch (std::bad_alloc &) {
      throw;
    } catch (std::exception &e) {
      // Cancellation and OOM are not this row's problem, and continuing would
      // keep writing to the store after the user asked us to stop
      if (dynamic_cast<InterruptException *>(&e) ||
          dynamic_cast<OutOfMemoryException *>(&e))
        throw;

      const string message = ErrorData(e).RawMessage();
      DUCKDB_LOG_ERROR(ctx, "Unable to delete from PST file (%s): %s", path,
                       message);
      results.push_back({path, target.node, target.owner, 0,
                         PSTDeleteStatus::Failed, message});

      // A write that failed partway leaves the store rewritten under a stale
      // header. IOException is here because dfile writes through DuckDB, so
      // pstsdk::write_error never actually reaches us
      if (dynamic_cast<pstsdk::database_corrupt *>(&e) ||
          dynamic_cast<pstsdk::write_error *>(&e) ||
          dynamic_cast<IOException *>(&e))
        poisoned = message;

      continue;
    }

    results.push_back(
        {path, target.node, target.owner, bytes_wiped,
         really ? PSTDeleteStatus::Deleted : PSTDeleteStatus::Previewed, ""});
  }

  if (!really || !writable)
    return;

  // Nothing is durable until this returns, so a failure here downgrades every
  // row this call already recorded rather than throwing the report away
  try {
    writable->sync();
  } catch (std::exception &e) {
    const string message = ErrorData(e).RawMessage();
    DUCKDB_LOG_ERROR(ctx, "Unable to flush PST file (%s): %s", path, message);

    for (idx_t r = first_result; r < results.size(); r++) {
      results[r].status = PSTDeleteStatus::Failed;
      results[r].error = message;
    }
  }
}

void PSTDeleteGlobalState::drain(ClientContext &ctx) {
  auto sync_buffer = buffer.synchronize();

  // Re-checked here, not just in bind: EXECUTE reuses a prepared statement's
  // bind data, so a statement prepared while the gate was on would otherwise
  // stay a working delete after RESET
  if (bind_data.really())
    check_gate(ctx, bind_data.function_name);

  for (idx_t i = 0; i < sync_buffer->paths.size(); i++) {
    const idx_t from = sync_buffer->drained_upto[i];
    if (from >= sync_buffer->targets[i].size())
      continue;

    drain_file(ctx, sync_buffer->paths[i], sync_buffer->targets[i], from,
               sync_buffer->results);
    sync_buffer->drained_upto[i] = sync_buffer->targets[i].size();
  }
}

idx_t PSTDeleteGlobalState::emit_rows(DataChunk &output) {
  auto sync_buffer = buffer.synchronize();

  idx_t rows = 0;
  while (rows < STANDARD_VECTOR_SIZE &&
         sync_buffer->emitted < sync_buffer->results.size()) {
    auto &result = sync_buffer->results[sync_buffer->emitted];

    idx_t col = 0;
    // Empty only when the input row's path was NULL, which has no path to name
    output.SetValue(col++, rows,
                    result.path.empty() ? Value(LogicalType::VARCHAR)
                                        : Value(result.path));

    if (bind_data.mode == PSTDeleteFunctionMode::FreeSpace) {
      output.SetValue(col++, rows,
                      result.status == PSTDeleteStatus::Deleted
                          ? Value::UBIGINT(result.bytes_wiped)
                          : Value(LogicalType::UBIGINT));
    } else if (bind_data.mode == PSTDeleteFunctionMode::Attachment) {
      output.SetValue(col++, rows, Value::UINTEGER(result.owner));
      output.SetValue(col++, rows, Value::UINTEGER(result.node));
    } else {
      output.SetValue(col++, rows, Value::UINTEGER(result.node));
    }

    output.SetValue(col++, rows,
                    Value::ENUM(static_cast<uint64_t>(result.status),
                                schema::DELETE_STATUS_ENUM));
    output.SetValue(col++, rows,
                    result.error.empty() ? Value(LogicalType::VARCHAR)
                                         : Value(result.error));

    sync_buffer->emitted++;
    rows++;
  }

  return rows;
}

const bool PSTDeleteGlobalState::finished() {
  auto sync_buffer = buffer.synchronize();
  return sync_buffer->emitted >= sync_buffer->results.size();
}

unique_ptr<GlobalTableFunctionState>
PSTDeleteInitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
  DUCKDB_LOG_DEBUG(ctx, "init_global [PSTDeleteInitGlobal]");
  return make_uniq<PSTDeleteGlobalState>(
      input.bind_data->Cast<PSTDeleteTableFunctionData>());
}

// Execute

OperatorResultType PSTDeleteFunction(ExecutionContext &ec,
                                     TableFunctionInput &data, DataChunk &input,
                                     DataChunk &output) {
  auto &bind_data = data.bind_data->Cast<PSTDeleteTableFunctionData>();
  auto &global_state = data.global_state->Cast<PSTDeleteGlobalState>();
  const bool attachments = bind_data.mode == PSTDeleteFunctionMode::Attachment;

  input.Flatten();

  for (idx_t row = 0; row < input.size(); row++) {
    auto path_value = input.GetValue(0, row);
    auto node_value = input.GetValue(attachments ? 2 : 1, row);

    const string path =
        path_value.IsNull() ? "" : path_value.GetValue<string>();
    const node_id node =
        node_value.IsNull() ? 0 : node_value.GetValue<node_id>();

    node_id owner = 0;
    if (attachments) {
      auto owner_value = input.GetValue(1, row);
      if (!owner_value.IsNull())
        owner = owner_value.GetValue<node_id>();

      if (owner_value.IsNull()) {
        global_state.buffer_failure(
            path, PSTDeleteTarget{node, owner},
            "message_node_id is NULL, so there is no message to delete from");
        continue;
      }
    }

    if (path_value.IsNull() || node_value.IsNull()) {
      global_state.buffer_failure(
          path, PSTDeleteTarget{node, owner},
          "a NULL path or node id has nothing to delete");
      continue;
    }

    // Writing an object back stores a new version, leaving the original, and
    // the data the caller wanted destroyed, in the bucket
    if (remote_path(path)) {
      global_state.buffer_failure(
          path, PSTDeleteTarget{node, owner},
          StringUtil::Format("cannot delete from '%s'. Deleting edits the PST "
                             "in place, which only works on a local file. Copy "
                             "it to local disk, delete from it there, then "
                             "upload the result",
                             path));
      continue;
    }

    global_state.buffer_target(path, PSTDeleteTarget{node, owner});
  }

  // Emitting nothing is what makes the finalize pass a barrier: no downstream
  // operator can be satisfied before the source scan has been drained
  output.SetCardinality(0);
  return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType PSTDeleteFinalFunction(ExecutionContext &ec,
                                                  TableFunctionInput &data,
                                                  DataChunk &output) {
  auto &global_state = data.global_state->Cast<PSTDeleteGlobalState>();

  global_state.drain(ec.client);
  output.SetCardinality(global_state.emit_rows(output));

  return global_state.finished() ? OperatorFinalizeResultType::FINISHED
                                 : OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
}

void PSTWipeFunction(ClientContext &ctx, TableFunctionInput &data,
                     DataChunk &output) {
  auto &global_state = data.global_state->Cast<PSTDeleteGlobalState>();

  global_state.drain(ctx);
  output.SetCardinality(global_state.emit_rows(output));
}

} // namespace intellekt::duckpst

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
  if (FileSystem::IsRemoteFile(path))
    throw InvalidInputException(
        "cannot wipe the remote path '%s'. Wiping a PST is an in place edit "
        "and only works on local files. Copy the file down, wipe the copy, "
        "then upload it",
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
  if (input.input_table_types.size() != want.size())
    throw InvalidInputException(
        "%s expects an input table of exactly %llu columns (%s), but received "
        "%llu. Project the columns explicitly, for example: SELECT * FROM "
        "%s((SELECT %s FROM read_pst_messages('x.pst') WHERE ...))",
        function_name, want.size(), signature, input.input_table_types.size(),
        function_name, StringUtil::Join(want_names, ", "));

  for (idx_t i = 0; i < want.size(); i++) {
    if (input.input_table_types[i] == want[i])
      continue;

    if (CastFunctionSet::ImplicitCastCost(ctx, input.input_table_types[i],
                                          want[i]) < 0)
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
      allow_delete.GetValue<bool>())
    return;

  throw InvalidInputException(
      "%s is disabled. Deleting from a PST edits the file in place and cannot "
      "be undone. Enable it with SET pst_allow_delete = true, then pass "
      "really := true to delete rather than preview.",
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

void PSTDeleteGlobalState::drain_file(ClientContext &ctx, const string &path,
                                      const vector<PSTDeleteTarget> &targets,
                                      idx_t from,
                                      vector<PSTDeleteResult> &results) {
  const bool really = bind_data.really();
  auto file = OpenFileInfo(path);

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
    DUCKDB_LOG_ERROR(ctx, "Unable to open PST file (%s): %s", path, e.what());
    for (idx_t t = from; t < targets.size(); t++)
      results.push_back({path, targets[t].node, targets[t].owner, 0,
                         PSTDeleteStatus::Failed, e.what()});
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
      switch (bind_data.mode) {
      case PSTDeleteFunctionMode::Message:
        if (really)
          pstsdk::delete_message(db, target.node);
        else
          db->lookup_node_info(target.node);
        break;
      case PSTDeleteFunctionMode::Folder:
        if (target.node == pstsdk::nid_root_folder ||
            target.node == pstsdk::nid_message_store)
          throw InvalidInputException(
              "refusing to delete node %u, which is the store's root",
              target.node);
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
    } catch (std::exception &e) {
      DUCKDB_LOG_ERROR(ctx, "Unable to delete from PST file (%s): %s", path,
                       e.what());
      results.push_back({path, target.node, target.owner, 0,
                         PSTDeleteStatus::Failed, e.what()});

      if (dynamic_cast<pstsdk::database_corrupt *>(&e) ||
          dynamic_cast<pstsdk::write_error *>(&e))
        poisoned = e.what();

      continue;
    }

    results.push_back(
        {path, target.node, target.owner, bytes_wiped,
         really ? PSTDeleteStatus::Deleted : PSTDeleteStatus::Previewed, ""});
  }

  if (really && writable)
    writable->sync();
}

void PSTDeleteGlobalState::drain(ClientContext &ctx) {
  auto sync_buffer = buffer.synchronize();

  for (idx_t i = 0; i < sync_buffer->paths.size(); i++) {
    const idx_t from = sync_buffer->drained_upto[i];
    if (from >= sync_buffer->targets[i].size())
      continue;

    sync_buffer->drained_upto[i] = sync_buffer->targets[i].size();
    drain_file(ctx, sync_buffer->paths[i], sync_buffer->targets[i], from,
               sync_buffer->results);
  }
}

idx_t PSTDeleteGlobalState::emit_rows(DataChunk &output) {
  auto sync_buffer = buffer.synchronize();

  idx_t rows = 0;
  while (rows < STANDARD_VECTOR_SIZE &&
         sync_buffer->emitted < sync_buffer->results.size()) {
    auto &result = sync_buffer->results[sync_buffer->emitted];

    idx_t col = 0;
    output.SetValue(col++, rows, Value(result.path));

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

    if (path_value.IsNull() || node_value.IsNull())
      throw InvalidInputException(
          "%s received a NULL in the input table at row %llu. A NULL path or "
          "node id has nothing to delete",
          bind_data.function_name, row);

    auto path = path_value.GetValue<string>();

    // Editing in place over object storage would rewrite the object and leave
    // the unscrubbed original in version history
    if (FileSystem::IsRemoteFile(path))
      throw InvalidInputException(
          "cannot delete from the remote path '%s'. Deleting from a PST is an "
          "in place edit and only works on local files. Copy the file down, "
          "delete from the copy, then upload it",
          path);

    node_id owner = 0;
    if (attachments) {
      auto owner_value = input.GetValue(1, row);
      if (owner_value.IsNull())
        throw InvalidInputException(
            "delete_pst_attachments received a NULL message_node_id at row "
            "%llu",
            row);
      owner = owner_value.GetValue<node_id>();
    }

    global_state.buffer_target(
        path, PSTDeleteTarget{node_value.GetValue<node_id>(), owner});
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

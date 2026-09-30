#include "duckdb/optimizer/optimizer_extension.hpp"
#include "filter_optimizer.hpp"
#if DUCDKB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_EXTENSION_MAIN
#endif

#include "delete_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "pst_extension.hpp"
#include "table_function.hpp"

namespace duckdb {
using namespace intellekt;

static string ReadDescription(duckpst::PSTReadFunctionMode mode) {
  switch (mode) {
  case duckpst::Folder:
    return "Read folders and their message counts from PST files.";
  case duckpst::Message:
    return "Read all messages from PST files with the base email fields.";
  case duckpst::Note:
    return "Read messages classified as notes from PST files.";
  case duckpst::Contact:
    return "Read contacts from PST files with contact fields.";
  case duckpst::Appointment:
    return "Read appointments from PST files with calendar fields.";
  case duckpst::StickyNote:
    return "Read sticky notes from PST files.";
  case duckpst::Task:
    return "Read tasks from PST files with task fields.";
  case duckpst::DistList:
    return "Read distribution lists from PST files with member fields.";
  default:
    throw InternalException("Missing PST read function description");
  }
}

static string DeleteDescription(duckpst::PSTDeleteFunctionMode mode) {
  switch (mode) {
  case duckpst::PSTDeleteFunctionMode::Message:
    return "Preview or delete PST messages and their attachments using "
           "(pst_path, node_id) rows.";
  case duckpst::PSTDeleteFunctionMode::Folder:
    return "Preview or delete PST folders and their contents using "
           "(pst_path, node_id) rows.";
  case duckpst::PSTDeleteFunctionMode::Attachment:
    return "Preview or delete PST attachments using (pst_path, "
           "message_node_id, attachment_node_id) rows while keeping the "
           "messages.";
  case duckpst::PSTDeleteFunctionMode::FreeSpace:
    return "Preview or wipe unused space in PST files matched by a path or "
           "pattern.";
  default:
    throw InternalException("Missing PST delete function description");
  }
}

static string DeleteExample(duckpst::PSTDeleteFunctionMode mode) {
  switch (mode) {
  case duckpst::PSTDeleteFunctionMode::Message:
    return "SELECT * FROM delete_pst_messages((SELECT pst_path, node_id FROM "
           "read_pst_messages('test/unittest.pst') LIMIT 1));";
  case duckpst::PSTDeleteFunctionMode::Folder:
    return "SELECT * FROM delete_pst_folders((SELECT pst_path, node_id FROM "
           "read_pst_folders('test/unittest.pst') WHERE display_name = "
           "'Notes'));";
  case duckpst::PSTDeleteFunctionMode::Attachment:
    return "SELECT * FROM delete_pst_attachments((SELECT m.pst_path, "
           "m.node_id, a.node_id FROM read_pst_messages('test/unittest.pst') "
           "m, UNNEST(m.attachments) t(a) LIMIT 1));";
  case duckpst::PSTDeleteFunctionMode::FreeSpace:
    return "SELECT * FROM wipe_pst_free_space('test/unittest.pst');";
  default:
    throw InternalException("Missing PST delete function example");
  }
}

static void RegisterDocumentedFunction(ExtensionLoader &loader,
                                       TableFunction function,
                                       const string &parameter_name,
                                       const string &description_text,
                                       const string &example,
                                       const string &category) {
  CreateTableFunctionInfo info(std::move(function));
  FunctionDescription description;
  description.parameter_names = {parameter_name};
  description.description = description_text;
  description.examples = {example};
  description.categories = {"PST", category};
  info.descriptions.push_back(std::move(description));
  info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));
}

static void LoadInternal(ExtensionLoader &loader) {
  auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());

  // Has to be here, not in Load: the loadable build enters through
  // DUCKDB_CPP_EXTENSION_ENTRY, which never calls Load
  OptimizerExtension::Register(config, duckpst::PSTFilterOptimizer());

  config.AddExtensionOption(
      "pst_allow_delete",
      "Allow delete_pst_* and wipe_pst_free_space to edit PST files in place. "
      "Deletion cannot be undone.",
      LogicalType::BOOLEAN, Value::BOOLEAN(false), nullptr, SetScope::SESSION);

  TableFunction proto("default", {LogicalType::VARCHAR},
                      duckpst::PSTReadFunction);

  proto.bind = duckpst::PSTReadBind;
  proto.cardinality = duckpst::PSTReadCardinality;
  proto.init_global = duckpst::PSTReadInitGlobal;
  proto.init_local = duckpst::PSTReadInitLocal;

  // Currently only used for basic count(*) pushdown
  proto.get_partition_info = duckpst::PSTPartitionInfo;
  proto.get_partition_stats = duckpst::PSTPartitionStats;

  proto.get_virtual_columns = duckpst::PSTVirtualColumns;
  proto.get_row_id_columns = duckpst::PSTRowIDColumns;

  proto.table_scan_progress = duckpst::PSTReadProgress;
  proto.dynamic_to_string = duckpst::PSTDynamicToString;

  proto.filter_pushdown = true;
  proto.projection_pushdown = true;
  proto.late_materialization = true;
  proto.pushdown_expression = duckpst::PSTPushdownExpression;

  proto.named_parameters = duckpst::NAMED_PARAMETERS;

  for (auto pair : duckpst::FUNCTIONS) {
    TableFunction concrete = proto;
    auto &[name, mode] = pair;

    concrete.name = name;
    RegisterDocumentedFunction(
        loader, std::move(concrete), "path", ReadDescription(mode),
        "SELECT * FROM " + name + "('test/unittest.pst');", "Read");
  }

  TableFunction delete_proto("default", {LogicalType::TABLE}, nullptr,
                             duckpst::PSTDeleteBind,
                             duckpst::PSTDeleteInitGlobal);

  delete_proto.in_out_function = duckpst::PSTDeleteFunction;
  delete_proto.in_out_function_final = duckpst::PSTDeleteFinalFunction;
  delete_proto.named_parameters = duckpst::DELETE_NAMED_PARAMETERS;

  // Targets are grouped per file, so output order does not follow input order
  delete_proto.order_preservation_type = OrderPreservationType::NO_ORDER;

  for (auto pair : duckpst::DELETE_FUNCTIONS) {
    auto &[name, mode] = pair;
    if (mode == duckpst::PSTDeleteFunctionMode::FreeSpace)
      continue;

    TableFunction concrete = delete_proto;
    concrete.name = name;
    RegisterDocumentedFunction(loader, std::move(concrete), "targets",
                               DeleteDescription(mode), DeleteExample(mode),
                               "Delete");
  }

  // A wipe takes a globbable path rather than a table of node ids
  TableFunction wipe("wipe_pst_free_space", {LogicalType::VARCHAR},
                     duckpst::PSTWipeFunction, duckpst::PSTWipeBind,
                     duckpst::PSTDeleteInitGlobal);

  wipe.named_parameters = duckpst::DELETE_NAMED_PARAMETERS;
  RegisterDocumentedFunction(
      loader, std::move(wipe), "path",
      DeleteDescription(duckpst::PSTDeleteFunctionMode::FreeSpace),
      DeleteExample(duckpst::PSTDeleteFunctionMode::FreeSpace), "Delete");
}

void PstExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

std::string PstExtension::Name() { return "pst"; }

std::string PstExtension::Version() const {
#ifdef EXT_VERSION_PST
  return EXT_VERSION_PST;
#else
  return "";
#endif
}
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(pst, loader) { duckdb::LoadInternal(loader); }
}

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "filter_optimizer.hpp"
#if DUCDKB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_EXTENSION_MAIN
#endif

#include "delete_function.hpp"
#include "table_function.hpp"
#include "pst_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
using namespace intellekt;

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
    auto &[name, _mode] = pair;

    concrete.name = name;
    loader.RegisterFunction(concrete);
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
    loader.RegisterFunction(concrete);
  }

  // A wipe takes a globbable path rather than a table of node ids
  TableFunction wipe("wipe_pst_free_space", {LogicalType::VARCHAR},
                     duckpst::PSTWipeFunction, duckpst::PSTWipeBind,
                     duckpst::PSTDeleteInitGlobal);

  wipe.named_parameters = duckpst::DELETE_NAMED_PARAMETERS;
  loader.RegisterFunction(wipe);
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

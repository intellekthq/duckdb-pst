#define DUCKDB_EXTENSION_MAIN

#include "table_function.hpp"
#include "pst_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
using namespace intellekt;

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction proto("default", {LogicalType::VARCHAR}, duckpst::PSTReadFunction);

	proto.bind = duckpst::PSTReadBind;
	proto.cardinality = duckpst::PSTReadCardinality;
	proto.init_global = duckpst::PSTReadInitGlobal;
	proto.init_local = duckpst::PSTReadInitLocal;

	// Currently only used for basic count(*) pushdown
	proto.get_partition_info = duckpst::PSTPartitionInfo;
	proto.get_partition_stats = duckpst::PSTPartitionStats;

	// For late materialization support, however we can't prune partitions
	// without `filter_pushdown=true` and handling row-by-row filters ourselves
	proto.get_virtual_columns = duckpst::PSTVirtualColumns;
	proto.get_row_id_columns = duckpst::PSTRowIDColumns;

	proto.table_scan_progress = duckpst::PSTReadProgress;
	proto.dynamic_to_string = duckpst::PSTDynamicToString;

	proto.late_materialization = true;
	proto.projection_pushdown = true;
	proto.named_parameters = duckpst::NAMED_PARAMETERS;

	for (auto pair : duckpst::FUNCTIONS) {
		TableFunction concrete = proto;
		auto &[name, _mode] = pair;

		concrete.name = name;
		loader.RegisterFunction(concrete);
	}
}

void PstExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string PstExtension::Name() {
	return "pst";
}

std::string PstExtension::Version() const {
#ifdef EXT_VERSION_PST
	return EXT_VERSION_PST;
#else
	return "";
#endif
}
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(pst, loader) {
	duckdb::LoadInternal(loader);
}
}

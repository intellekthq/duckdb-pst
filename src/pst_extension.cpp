#define DUCKDB_EXTENSION_MAIN

#include "table_function.hpp"
#include "pst_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {
using namespace intellekt;

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction proto("default", {LogicalType::VARCHAR}, duckpst::PSTReadFunction);

	proto.bind = duckpst::PSTReadBind;
	proto.cardinality = duckpst::PSTReadCardinality;
	proto.init_global = duckpst::PSTReadInitGlobal;
	proto.init_local = duckpst::PSTReadInitLocal;
	proto.table_scan_progress = duckpst::PSTReadProgress;
	proto.projection_pushdown = true;
	proto.get_partition_stats = duckpst::PSTPartitionStats;

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

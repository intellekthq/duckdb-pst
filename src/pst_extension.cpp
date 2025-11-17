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
	TableFunction read_pst_folders("read_pst_folders", {LogicalType::VARCHAR}, duckpst::PSTReadFunction,
	                               duckpst::PSTReadBind, duckpst::PSTReadInitGlobal, duckpst::PSTReadInitLocal);
	read_pst_folders.cardinality = duckpst::PSTReadCardinality;

	TableFunction read_pst_messages("read_pst_messages", {LogicalType::VARCHAR}, duckpst::PSTReadFunction,
	                                duckpst::PSTReadBind, duckpst::PSTReadInitGlobal, duckpst::PSTReadInitLocal);
	read_pst_messages.cardinality = duckpst::PSTReadCardinality;

	loader.RegisterFunction(read_pst_folders);
	loader.RegisterFunction(read_pst_messages);
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

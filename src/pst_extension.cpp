#define DUCKDB_EXTENSION_MAIN

#include "pst_table_function.hpp"
#include "pst_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {
	using namespace intellekt;

	static void LoadInternal(ExtensionLoader &loader) {
		TableFunction read_pst("read_pst", {LogicalType::VARCHAR}, duckpst::PSTReadFunction, duckpst::PSTReadBind);

		loader.RegisterFunction(read_pst);
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
}

extern "C" {
	DUCKDB_CPP_EXTENSION_ENTRY(pst, loader) {
		duckdb::LoadInternal(loader);
	}
}

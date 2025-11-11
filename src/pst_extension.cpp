#define DUCKDB_EXTENSION_MAIN

#include "pst_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void PstScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "PST " + name.GetString() + " 📧");
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto pst_scalar_function = ScalarFunction("pst", {LogicalType::VARCHAR}, LogicalType::VARCHAR, PstScalarFun);
	loader.RegisterFunction(pst_scalar_function);
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

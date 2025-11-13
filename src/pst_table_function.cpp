#include "pst_table_function.hpp"
#include "utils.hpp"

namespace intellekt::duckpst {
    using namespace duckdb;
    using namespace pstsdk;

    PSTReadTableData::PSTReadTableData(string &path) : pst(utils::to_wstring(path)) {}

    unique_ptr<FunctionData> PSTReadBind(
        ClientContext &ctx,
        TableFunctionBindInput &input,
        vector<LogicalType> &return_types,
        vector<string> &names) {
            auto path = input.inputs[0].GetValue<string>();
            auto bind_data = make_uniq<PSTReadTableData>(path);
            return bind_data;
    }

    void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output) {

    }
}
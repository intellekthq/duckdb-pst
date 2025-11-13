#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include <pstsdk/pst.h>


namespace intellekt::duckpst {
    // using namespace std;
    using namespace duckdb;
    using namespace pstsdk;

    struct PSTReadTableData : public TableFunctionData {
        pst pst;

        PSTReadTableData(string &path);
    };

    unique_ptr<FunctionData> PSTReadBind(ClientContext &ctx, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
    void PSTReadFunction(ClientContext &ctx, TableFunctionInput &input, DataChunk &output);
}

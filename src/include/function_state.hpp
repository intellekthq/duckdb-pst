#pragma once

#include "duckdb.hpp"

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/table_function.hpp"
#include "table_function.hpp"

#include <boost/thread/synchronized_value.hpp>
#include <pstsdk/pst.h>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

class PSTReadGlobalTableFunctionState : public GlobalTableFunctionState {
	boost::synchronized_value<queue<PSTInputPartition>> partitions;

public:
	PSTReadGlobalTableFunctionState(const PSTReadTableFunctionData &bind_data, vector<column_t> column_ids);
	const PSTReadTableFunctionData &bind_data;

	std::optional<PSTInputPartition> take_partition();

	idx_t nodes_processed;
	vector<column_t> column_ids;
	idx_t MaxThreads() const override;
};

class PSTIteratorLocalTableFunctionState : public LocalTableFunctionState {
protected:
	PSTIteratorLocalTableFunctionState(PSTReadGlobalTableFunctionState &global_state, ExecutionContext &ec);
	bool bind_partition();

public:
	ExecutionContext &ec;
	PSTReadGlobalTableFunctionState &global_state;

	std::optional<pstsdk::pst> pst;
	std::optional<PSTInputPartition> partition;

	virtual idx_t emit_rows(DataChunk &output) {
		return 0;
	}

	const vector<column_t> &column_ids();
	const LogicalType &output_schema();
};

typedef vector<node_id>::iterator node_id_iterator;

template <typename t>
class PSTConcreteIteratorState : public PSTIteratorLocalTableFunctionState {
	t current_item();

protected:
	std::optional<node_id_iterator> current;
	std::optional<node_id_iterator> end;

	bool bind_next();

public:
	PSTConcreteIteratorState(PSTReadGlobalTableFunctionState &global_state, ExecutionContext &ec);

	const bool finished();

	std::optional<t> next();
	idx_t emit_rows(DataChunk &output) override;
};

} // namespace intellekt::duckpst

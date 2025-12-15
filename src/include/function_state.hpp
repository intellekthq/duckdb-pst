#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/table_function.hpp"
#include "pst/typed_bag.hpp"
#include "table_function.hpp"

#include <boost/thread/synchronized_value.hpp>
#include <pstsdk/pst.h>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

/**
 * The global PST read state is a queue of input partitions, where the progress
 * of the read is determined by the number of NDB nodes spooled.
 */
class PSTReadGlobalState : public GlobalTableFunctionState {
	boost::synchronized_value<queue<PSTInputPartition>> partitions;

public:
	PSTReadGlobalState(const PSTReadTableFunctionData &bind_data, vector<column_t> column_ids);
	const PSTReadTableFunctionData &bind_data;

	std::optional<PSTInputPartition> take_partition();

	idx_t nodes_processed;
	vector<column_t> column_ids;
	idx_t MaxThreads() const override;
};

typedef vector<node_id>::iterator node_id_iterator;

/**
 * The local (per-thread) read state spools node_ids out of a partition,
 * asking for a new one after all nodes have been output.
 */
class PSTReadLocalState : public LocalTableFunctionState {
protected:
	PSTReadLocalState(PSTReadGlobalState &global_state, ExecutionContext &ec);

	std::optional<node_id_iterator> current;
	std::optional<node_id_iterator> end;

	/**
	 * @brief Dequeue a partition from global state
	 *
	 * @return true A partition was received
	 * @return false No partitions are available
	 */
	bool bind_partition();

	bool bind_next();

public:
	ExecutionContext &ec;
	PSTReadGlobalState &global_state;

	std::optional<pstsdk::pst> pst;
	std::optional<PSTInputPartition> partition;

	/**
	 * @brief Is this partition done?
	 *
	 * @return true
	 * @return false
	 */
	const bool finished();

	/**
	 * @brief Spools rows into DataChunk
	 *
	 * @param output Current data chunk
	 * @return idx_t Number of rows
	 */
	virtual idx_t emit_rows(DataChunk &output) = 0;

	const vector<column_t> &column_ids();
	const LogicalType &output_schema();
};

template <pst::MessageClass V, typename T = pstsdk::message>
class PSTReadConcreteLocalState : public PSTReadLocalState {
public:
	PSTReadConcreteLocalState(PSTReadGlobalState &global_state, ExecutionContext &ec);

	virtual idx_t emit_rows(DataChunk &output) override;

	/**
	 * @brief Get the next item and move the iterator
	 *
	 * @return std::optional<t>
	 */
	std::optional<pst::TypedBag<V, T>> next();
};

} // namespace intellekt::duckpst

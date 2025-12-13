#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/table_function.hpp"
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

/**
 * The local (per-thread) read state spools node_ids out of a partition,
 * asking for a new one after all nodes have been output.
 */
class PSTReadLocalState : public LocalTableFunctionState {
protected:
	PSTReadLocalState(PSTReadGlobalState &global_state, ExecutionContext &ec);

	/**
	 * @brief Dequeue a partition from global state
	 *
	 * @return true A partition was received
	 * @return false No partitions are available
	 */
	bool bind_partition();

public:
	ExecutionContext &ec;
	PSTReadGlobalState &global_state;

	std::optional<pstsdk::pst> pst;
	std::optional<PSTInputPartition> partition;

	/**
	 * @brief Spools rows into DataChunk
	 *
	 * @param output Current data chunk
	 * @return idx_t Number of rows
	 */
	virtual idx_t emit_rows(DataChunk &output) {
		return 0;
	}

	const vector<column_t> &column_ids();
	const LogicalType &output_schema();
};

typedef vector<node_id>::iterator node_id_iterator;

/**
 * Concrete local state implementation for a specific pstsdk type
 *
 * @tparam t pstsdk item type
 */
template <typename t>
class PSTReadRowSpoolerState : public PSTReadLocalState {
	t current_item();
	node_id current_node_id();

protected:
	std::optional<node_id_iterator> current;
	std::optional<node_id_iterator> end;

	bool bind_next();

public:
	PSTReadRowSpoolerState(PSTReadGlobalState &global_state, ExecutionContext &ec);

	/**
	 * @brief Is this partition done?
	 *
	 * @return true
	 * @return false
	 */
	const bool finished();

	/**
	 * @brief Get the next item and move the iterator
	 *
	 * @return std::optional<t>
	 */
	std::optional<t> next();

	/**
	 * @brief Concrete row spooler for `t`
	 *
	 * @param output
	 * @return idx_t
	 */
	idx_t emit_rows(DataChunk &output) override;
};

} // namespace intellekt::duckpst

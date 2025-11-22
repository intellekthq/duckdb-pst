#pragma once

#include "duckdb.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "table_function.hpp"

#include <boost/thread/synchronized_value.hpp>
#include <pstsdk/pst.h>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// The global state for any PST read is a queue of files
class PSTReadGlobalTableFunctionState : public GlobalTableFunctionState {
	boost::synchronized_value<queue<OpenFileInfo>> files;
	queue<queue<node_id>> folder_ids;

	// TODO
	boost::synchronized_value<std::optional<queue<node_id>>> current_message_ids;
	std::optional<OpenFileInfo> current_file;

	idx_t bind_message_ids();

public:
	const LogicalType &output_schema;
	const PSTReadFunctionMode mode;

	PSTReadGlobalTableFunctionState(queue<OpenFileInfo> &&files, queue<queue<node_id>> &&folder_queue,
	                                const PSTReadFunctionMode mode, const LogicalType &output_schema);

	idx_t MaxThreads() const override;
	std::optional<std::pair<OpenFileInfo, node_id>> take();
	std::optional<std::pair<OpenFileInfo, vector<node_id>>> take_n(idx_t n);
};

// The local state has a PST with a reference to the global state. In the case
// that there are more files than threads (small reads), the local state can
// dequeue another file for processing.

// The local state is responsible for spooling rows into a DataChunk ref. Concrete
// impls of the iterator state override for their own emit_rows behavior. The output schema
// is resolved via the global state ref, and bound by the `PSTReadTableFunctionData` constructor.

// Concrete impls must be provided for each iterator type we wish to emit. Currently
// this is just folders (pst::folder_iterator, pstsdk::folder)
// and messages (pst::message_iterator, pstsdk::message). In pstsdk, the iterators for
// ndb backed iterators and table backed iterators have different types (so e.g. subfolders need
// their own concrete implementations).
class PSTIteratorLocalTableFunctionState : public LocalTableFunctionState {
protected:
	std::optional<pst> pst;
	std::optional<node_id> folder_id;

	OpenFileInfo file;
	PSTReadGlobalTableFunctionState &global_state;
	PSTIteratorLocalTableFunctionState(PSTReadGlobalTableFunctionState &global_state);

public:
	virtual idx_t emit_rows(DataChunk &output) {
		return 0;
	}

	const OpenFileInfo &current_file();
	const std::optional<class pst> &current_pst();
	const LogicalType &output_schema();
};

template <typename it, typename t>
class PSTConcreteIteratorState : public PSTIteratorLocalTableFunctionState {
	t current_item();

protected:
	std::optional<it> current;
	std::optional<it> end;
	std::optional<vector<node_id>> batch;

	bool bind_next();
	void bind_iter();

public:
	PSTConcreteIteratorState(PSTReadGlobalTableFunctionState &global_state);

	const bool finished();

	std::optional<t> next();
	idx_t emit_rows(DataChunk &output) override;
};

} // namespace intellekt::duckpst

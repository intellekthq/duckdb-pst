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

// The global state for a PST read is initialized from a list of globbed files, which is
// turned into a mutex'd queue, exposing iteration via:
//
// - take_file()     -> a PST at a time
// - take_folder()  -> a directory at a time within a PST
// - take_message() -> a message at a time within a PST folder
//
// Folders for each PST are eagerly enumerated during init of bind_data to be available
// for cardinality estimates.
//
// Messages are lazily enumerated on demand while spooling via take*.
//
class PSTReadGlobalTableFunctionState : public GlobalTableFunctionState {
	// take_file
	boost::synchronized_value<queue<OpenFileInfo>> files;

	// take_folder
	queue<queue<node_id>> folder_ids;

	// take_messages
	std::optional<OpenFileInfo> current_file;
	boost::synchronized_value<std::optional<queue<node_id>>> current_message_ids;

	// progress
	idx_t total_files;
	idx_t total_messages;

	int64_t bind_message_ids();
public:
	PSTReadGlobalTableFunctionState(PSTReadTableFunctionData &bind_data);
	PSTReadFunctionMode mode;

	idx_t MaxThreads() const override;
	double progress() const;

	std::optional<OpenFileInfo> take_file();
	std::optional<std::pair<OpenFileInfo, node_id>> take_folder();
	std::optional<std::pair<OpenFileInfo, vector<node_id>>> take_messages(idx_t n);
};

// PSTIteratorLocalTableFunctionState is a generic data bag that holds a PST instance,
// folder ID, or list of message IDs depending on the scan being executed. Reads are
// parallelized: several threads can process parts of a single large file, or different
// files altogether.

// The local state is responsible for spooling rows into a DataChunk ref. Concrete
// impls of the iterator state override for their own emit_rows behavior. The output schema
// can be confirmed in the global state, but is 1:1 with the item type.

// Concrete impls must be provided for each iterator type we wish to spool. In pstsdk,
// the iterators for ndb backed iterators and table backed iterators have different types
// (so e.g. subfolders need their own concrete implementations).
//
class PSTIteratorLocalTableFunctionState : public LocalTableFunctionState {
protected:
	std::optional<pst> pst;
	std::optional<node_id> folder_id;
	std::optional<vector<node_id>> message_batch;

	OpenFileInfo file;
	PSTReadGlobalTableFunctionState &global_state;
	PSTIteratorLocalTableFunctionState(PSTReadGlobalTableFunctionState &global_state);

public:
	virtual idx_t emit_rows(DataChunk &output) {
		return 0;
	}

	const OpenFileInfo &current_file();
	const std::optional<class pst> &current_pst();
};

template <typename it, typename t>
class PSTConcreteIteratorState : public PSTIteratorLocalTableFunctionState {
	t current_item();

protected:
	std::optional<it> current;
	std::optional<it> end;

	bool bind_next();
	void bind_iter();

public:
	PSTConcreteIteratorState(PSTReadGlobalTableFunctionState &global_state);

	const bool finished();

	std::optional<t> next();
	idx_t emit_rows(DataChunk &output) override;
};

} // namespace intellekt::duckpst

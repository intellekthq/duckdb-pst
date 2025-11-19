#include "duckdb.hpp"
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

public:
	const LogicalType &output_schema;
	const PSTReadFunctionMode mode;

	PSTReadGlobalTableFunctionState(queue<OpenFileInfo> &&files, queue<queue<node_id>> &&folder_queue,
	                                const PSTReadFunctionMode mode, const LogicalType &output_schema);

	// This is an upper limit, where we are saying we're happy with one
	// thread per file. The optimizer uses this and the cardinality estimate
	// to determine how many threads to spawn.

	// NOTE: One thread per file is not guaranteed!
	idx_t MaxThreads() const override;
	std::optional<std::pair<OpenFileInfo, node_id>> take();
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
	PSTIteratorLocalTableFunctionState(OpenFileInfo &&file, PSTReadGlobalTableFunctionState &global_state);
	PSTIteratorLocalTableFunctionState(OpenFileInfo &&file, std::optional<node_id> &&maybe_folder_id,
	                                   PSTReadGlobalTableFunctionState &global_state);

public:
	virtual idx_t emit_rows(DataChunk &output) {
		return 0;
	}
};

template <typename it, typename t>
class PSTConcreteIteratorState : public PSTIteratorLocalTableFunctionState {
	std::optional<it> current;
	std::optional<it> end;

	bool bind_next();
	void bind_iter();

public:
	PSTConcreteIteratorState(OpenFileInfo &&file, PSTReadGlobalTableFunctionState &global_state);
	PSTConcreteIteratorState(OpenFileInfo &&file, std::optional<node_id> &&maybe_folder_id,
	                         PSTReadGlobalTableFunctionState &global_state);

	const LogicalType &output_schema();
	const OpenFileInfo &current_file();
	const std::optional<class pst> &current_pst();
	const bool finished();

	std::optional<t> next();
	idx_t emit_rows(DataChunk &output) override;
};
} // namespace intellekt::duckpst

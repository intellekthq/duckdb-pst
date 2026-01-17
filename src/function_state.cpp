#include "function_state.hpp"

#include "duckdb/function/table_function.hpp"
#include "pst/typed_bag.hpp"
#include "row_serializer.hpp"
#include "table_function.hpp"

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/logging/logger.hpp"

#include <optional>
#include <utility>

namespace intellekt::duckpst {
using namespace duckdb;
using namespace pstsdk;

// PSTReadGlobalState
PSTReadGlobalState::PSTReadGlobalState(
    const PSTReadTableFunctionData &bind_data,
    const TableFunctionInitInput &input)
    : bind_data(bind_data), column_ids(input.column_ids) {
  nodes_processed = 0;
  nonempty_partition_count = 0;

  set<OpenFileInfo> unique_files_read;

  auto sync_partitions = partitions.synchronize();
  for (auto partition : bind_data.partitions.get()) {

    if (input.filters) {
      for (auto &[column_id, f] : input.filters->filters) {
        auto schema_col = input.column_ids[column_id];
        switch (schema_col) {
        case schema::PST_VCOL_PARTITION_INDEX:
        case schema::PST_VCOL_NODE_ID: {
          auto new_nodes = partition.prune(schema_col, f);
          partition.nodes = {new_nodes.begin(), new_nodes.end()};
          break;
        }
        default:
          break;
        }
      }
    }

    partition.stats.count = partition.nodes.size();

    if (!partition.nodes.empty()) {
      partition.stats.row_start = partition.nodes[0];
      nonempty_partition_count += 1;
      unique_files_read.emplace(partition.file);
    }

    sync_partitions->push(partition);
  }

  files_read = unique_files_read.size();
}

std::optional<PSTInputPartition> PSTReadGlobalState::take_partition() {
  auto sync_partitions = partitions.synchronize();
  if (sync_partitions->empty())
    return {};

  auto part = sync_partitions->front();

  // TODO: it would be more honest if this happened after emission
  nodes_processed += part.stats.count;

  sync_partitions->pop();
  return std::move(part);
}

idx_t PSTReadGlobalState::MaxThreads() const {
  return std::max<idx_t>(partitions->size(), 1);
}

// PSTReadLocalState
PSTReadLocalState::PSTReadLocalState(PSTReadGlobalState &global_state,
                                     ExecutionContext &ec)
    : global_state(global_state), ec(ec) {
  bind_partition();
  if (partition.has_value()) {
    current.emplace(partition->nodes.begin());
    end.emplace(partition->nodes.end());
  }
}

bool PSTReadLocalState::bind_partition() {
  auto next_partition = global_state.take_partition();
  if (!next_partition.has_value())
    return false;

  bool skip_bind_pst = partition.has_value() &&
                       (next_partition->file.path == partition->file.path);
  partition.emplace(std::move(*next_partition));
  if (!skip_bind_pst)
    pst.emplace(pstsdk::pst(*partition->pst));

  return true;
}

const vector<column_t> &PSTReadLocalState::column_ids() {
  return global_state.column_ids;
}

const LogicalType &PSTReadLocalState::output_schema() {
  return duckpst::output_schema(global_state.bind_data.mode);
}

const bool PSTReadLocalState::finished() {
  return (!current) || (current == end);
}

bool PSTReadLocalState::bind_next() {
  while (finished()) {
    if (!bind_partition())
      return false;
    current.emplace(partition->nodes.begin());
    end.emplace(partition->nodes.end());
  }

  return true;
}

// PSTReadConcreteLocalState
template <pst::MessageClass V, typename T>
PSTReadConcreteLocalState<V, T>::PSTReadConcreteLocalState(
    PSTReadGlobalState &global_state, ExecutionContext &ec)
    : PSTReadLocalState(global_state, ec) {}

template <pst::MessageClass V, typename T>
std::optional<pst::TypedBag<V, T>> PSTReadConcreteLocalState<V, T>::next() {
  // If the current state is finished, keep going until we can keep binding
  while (finished() && bind_next()) {
  }

  // If we can't bind anymore and are finished, we're really finished
  if (finished())
    return {};

  pst::TypedBag<V, T> typed_bag(*pst, **current);

  ++(*current);
  return typed_bag;
}

template <pst::MessageClass V, typename T>
idx_t PSTReadConcreteLocalState<V, T>::emit_rows(DataChunk &output) {
  idx_t rows = 0;

  for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
    auto item = next();

    if (!item) {
      break;
    }

    row_serializer::into_row<pst::TypedBag<V, T>>(*this, output, *item, i);

    ++rows;
  }

  return rows;
}

template class PSTReadConcreteLocalState<pst::MessageClass::Note,
                                         pstsdk::folder>;
template class PSTReadConcreteLocalState<pst::MessageClass::Note>;
template class PSTReadConcreteLocalState<pst::MessageClass::Contact>;
template class PSTReadConcreteLocalState<pst::MessageClass::Appointment>;
template class PSTReadConcreteLocalState<pst::MessageClass::StickyNote>;
template class PSTReadConcreteLocalState<pst::MessageClass::Task>;
template class PSTReadConcreteLocalState<pst::MessageClass::DistList>;

} // namespace intellekt::duckpst

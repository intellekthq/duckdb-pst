#include "filter_optimizer.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/logical_tokens.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "schema.hpp"
#include "table_function.hpp"
#include <optional>

namespace intellekt::duckpst {
map<idx_t, unique_ptr<TableFilter>>
PSTFilterOptimizer::CollectAndEraseNonVirtualColumnFilters(
    LogicalGet &pst_logical_get) {
  map<idx_t, unique_ptr<TableFilter>> non_vcol_filters;

  for (auto &[schema_col, f] : pst_logical_get.table_filters.filters) {
    if (schema_col >= schema::DUCKDB_VIRTUAL_COLUMN_START)
      continue;

    non_vcol_filters.insert({schema_col, f->Copy()});
  }

  for (auto &[schema_col, _] : non_vcol_filters) {
    pst_logical_get.table_filters.filters.erase(schema_col);
  }

  return non_vcol_filters;
}

unique_ptr<LogicalFilter> PSTFilterOptimizer::CreateLogicalFilter(
    map<idx_t, unique_ptr<TableFilter>> &non_vcol_filters,
    LogicalGet &pst_logical_get) {
  auto logical_filter = make_uniq<LogicalFilter>();
  for (auto &[schema_col, tfilter] : non_vcol_filters) {
    std::optional<idx_t> projection_id;
    for (idx_t pid = 0; pid < pst_logical_get.GetColumnIds().size(); ++pid) {
      if (pst_logical_get.GetColumnIds()[pid].GetPrimaryIndex() == schema_col) {
        projection_id.emplace(pid);
        break;
      }
    }

    // Must be able to bind positional column ID
    if (!projection_id)
      continue;

    logical_filter->expressions.emplace_back(
        tfilter->ToExpression(BoundColumnRefExpression(
            pst_logical_get.names[schema_col],
            pst_logical_get.returned_types[schema_col],
            ColumnBinding(pst_logical_get.table_index, *projection_id))));
  }

  return logical_filter;
}

void PSTFilterOptimizer::RewriteEligiblePSTFilters(
    OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
  if (!plan)
    return;

  for (idx_t i = 0; i < plan->children.size(); ++i) {
    auto &child = plan->children[i];

    // Find a PST table function read
    if (child->type != LogicalOperatorType::LOGICAL_GET)
      continue;
    auto &pst_logical_get = child->Cast<LogicalGet>();
    auto maybe_pst_function_mode =
        duckpst::FUNCTIONS.find(pst_logical_get.function.name);
    if (maybe_pst_function_mode == duckpst::FUNCTIONS.end())
      continue;

    // Collect non virtual column filters, erasing them from the LOGICAL_GET
    // table filters If there are none, then there is nothing to rewrite
    auto non_vcol_filters =
        CollectAndEraseNonVirtualColumnFilters(pst_logical_get);
    if (non_vcol_filters.empty())
      continue;

    // Create a new filter node using the non virtual column filters, adding the
    // PST UDTF call as a child
    auto non_vcol_filter =
        CreateLogicalFilter(non_vcol_filters, pst_logical_get);

    non_vcol_filter->AddChild(pst_logical_get.Copy(input.context));

    // Finally: do the rewrite
    plan->children[i] = std::move(non_vcol_filter);
  }

  for (auto &child : plan->children) {
    RewriteEligiblePSTFilters(input, child);
  }
}

PSTFilterOptimizer::PSTFilterOptimizer() {
  optimize_function = RewriteEligiblePSTFilters;
}
} // namespace intellekt::duckpst

#include "filter_optimizer.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/logical_tokens.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
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

unique_ptr<LogicalProjection>
PSTFilterOptimizer::CreateProjection(idx_t new_table_index,
                                     LogicalGet &pst_logical_get) {
  auto bound_column_ids = pst_logical_get.GetColumnIds();
  auto column_bindings = pst_logical_get.GetColumnBindings();
  vector<unique_ptr<Expression>> input_column_refs;

  // To make mapping easier, the new projection is 1:1 with the logical get
  // output
  for (idx_t column_id = 0; column_id < bound_column_ids.size(); ++column_id) {
    auto column_index = bound_column_ids[column_id];
    input_column_refs.emplace_back(make_uniq<BoundColumnRefExpression>(
        pst_logical_get.GetColumnName(column_index),
        pst_logical_get.GetColumnType(column_index),
        column_bindings[column_id]));
  }

  return std::move(make_uniq<LogicalProjection>(new_table_index,
                                                std::move(input_column_refs)));
}

unique_ptr<LogicalFilter> PSTFilterOptimizer::CreateLogicalFilter(
    map<idx_t, unique_ptr<TableFilter>> &non_vcol_filters,
    LogicalGet &pst_logical_get, LogicalProjection &pst_expanded_projection) {
  auto logical_filter = make_uniq<LogicalFilter>();
  for (auto &[schema_col, tfilter] : non_vcol_filters) {
    std::optional<idx_t> projection_id;
    for (idx_t pid = 0; pid < pst_logical_get.GetColumnIds().size(); ++pid) {
      if (pst_logical_get.GetColumnIds()[pid].GetPrimaryIndex() == schema_col) {
        projection_id.emplace(pid);
        break;
      }
    }

    if (!projection_id)
      continue;
    logical_filter->expressions.emplace_back(
        tfilter->ToExpression(BoundColumnRefExpression(
            pst_logical_get.names[schema_col],
            pst_logical_get.returned_types[schema_col],
            ColumnBinding(pst_expanded_projection.table_index,
                          *projection_id))));
  }

  return logical_filter;
}

void PSTFilterOptimizer::RewriteEligiblePSTFilters(
    OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
    map<idx_t, idx_t> &binding_map) {
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

    // Ask the binder for a new table index and create a widened projection
    // we can use in a subsequent filter operator
    auto projection_table_index = input.optimizer.binder.GenerateTableIndex();
    auto expanded_projection =
        CreateProjection(projection_table_index, pst_logical_get);
    expanded_projection->children.emplace_back(
        pst_logical_get.Copy(input.context));

    binding_map.insert(
        {pst_logical_get.table_index, expanded_projection->table_index});

    // Create a new filter node using the non vcol table filters, binding
    // against the expanded projection
    auto non_vcol_filter = CreateLogicalFilter(
        non_vcol_filters, pst_logical_get, *expanded_projection);
    non_vcol_filter->AddChild(std::move(expanded_projection));

    // Finally: do the rewrite
    plan->children[i] = std::move(non_vcol_filter);
  }

  for (auto &child : plan->children) {
    RewriteEligiblePSTFilters(input, child, binding_map);
  }
}

void PSTFilterOptimizer::UpdateTableBindings(
    map<idx_t, idx_t> &binding_map, unique_ptr<LogicalOperator> &plan) {
  LogicalOperatorVisitor::EnumerateExpressions(
      *plan, [&](unique_ptr<Expression> *expr) {
        if ((*expr)->type != ExpressionType::BOUND_COLUMN_REF)
          return;
        auto &bound_col_ref = (*expr)->Cast<BoundColumnRefExpression>();
        auto maybe_new_table_index =
            binding_map.find(bound_col_ref.binding.table_index);

        if (maybe_new_table_index == binding_map.end())
          return;
        auto new_table_index = maybe_new_table_index->second;

        // The original logical get table bindings still serve as inputs to the
        // new rewritten projection, so if the target table index is used by
        // this plan node, skip it
        for (auto plan_table_index : plan->GetTableIndex()) {
          if (new_table_index == plan_table_index)
            return;
        }

        bound_col_ref.binding.table_index = maybe_new_table_index->second;
      });

  for (auto &child : plan->children) {
    UpdateTableBindings(binding_map, child);
  }
}

void PSTFilterOptimizer::EnsureNonVirtualColumnFilters(
    OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
  map<idx_t, idx_t> binding_map;
  RewriteEligiblePSTFilters(input, plan, binding_map);

  if (binding_map.empty())
    return;
  UpdateTableBindings(binding_map, plan);
}

PSTFilterOptimizer::PSTFilterOptimizer() {
  optimize_function = EnsureNonVirtualColumnFilters;
}
} // namespace intellekt::duckpst

#pragma once
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/table_filter.hpp"
#include <optional>

namespace intellekt::duckpst {
using namespace duckdb;

class PSTFilterOptimizer : public OptimizerExtension {
    // static std::optional<unique_ptr<LogicalFilter>> CreateLogicalFilter(LogicalGet *pst_logical_get);
    static unique_ptr<LogicalProjection> CreateProjection(idx_t new_table_index, LogicalGet &pst_logical_get);
    static map<idx_t, unique_ptr<TableFilter>> CollectAndEraseNonVirtualColumnFilters(LogicalGet &pst_logical_get);
    static std::optional<LogicalGet *> FindPSTOperator(unique_ptr<LogicalOperator> &plan);
    static void RewriteEligiblePSTFilters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan, map<idx_t, vector<ColumnBinding>> &projection_rewrites);
    static void UpdateProjectionBindings(unique_ptr<LogicalOperator> &plan, map<idx_t, vector<ColumnBinding>> &projection_rewrites);
    static void EnsureNonVirtualColumnFilters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
public:
    PSTFilterOptimizer();
};

}
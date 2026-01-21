#pragma once
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace intellekt::duckpst {
using namespace duckdb;

class PSTFilterOptimizer : public OptimizerExtension {
  static map<idx_t, unique_ptr<TableFilter>>
  CollectAndEraseNonVirtualColumnFilters(LogicalGet &pst_logical_get);
  static unique_ptr<LogicalFilter>
  CreateLogicalFilter(map<idx_t, unique_ptr<TableFilter>> &non_vcol_filters,
                      LogicalGet &pst_logical_get);
  static void RewriteEligiblePSTFilters(OptimizerExtensionInput &input,
                                        unique_ptr<LogicalOperator> &plan);

public:
  PSTFilterOptimizer();
};

} // namespace intellekt::duckpst

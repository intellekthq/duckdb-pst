#pragma once
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace intellekt::duckpst {
using namespace duckdb;

/**
 * @brief Splits non virtual column filters out of LOGICAL_GET PST scans
 *        and into their own filter nodes
 *
 */
class PSTFilterOptimizer : public OptimizerExtension {
  /**
   * @brief Collect non virtual column `TableFilter`s from PST scans into a map
   *        and erase them from the operator.
   *
   * @param pst_logical_get The PST LOGICAL_GET
   * @return map<idx_t, unique_ptr<TableFilter>> A map of {schema_col ->
   * TableFilter}
   */
  static map<idx_t, unique_ptr<TableFilter>>
  CollectAndEraseNonVirtualColumnFilters(LogicalGet &pst_logical_get);

  /**
   * @brief Create a filter from a map of {schema_col -> TableFilter}
   *
   * @param non_vcol_filters
   * @param pst_logical_get
   * @return unique_ptr<LogicalFilter>
   */
  static unique_ptr<LogicalFilter>
  CreateLogicalFilter(map<idx_t, unique_ptr<TableFilter>> &non_vcol_filters,
                      LogicalGet &pst_logical_get);

  /**
   * @brief Walk the plan and split non-pushdown filters out of all PST
   * LOGICAL_GET nodes
   *
   * @param input
   * @param plan
   */
  static void RewriteEligiblePSTFilters(OptimizerExtensionInput &input,
                                        unique_ptr<LogicalOperator> &plan);

public:
  PSTFilterOptimizer();
};

} // namespace intellekt::duckpst

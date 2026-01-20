#include "filter_optimizer.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
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
    std::optional<LogicalGet *> PSTFilterOptimizer::FindPSTOperator(unique_ptr<LogicalOperator> &plan) {
        if (!plan) return {};

        switch (plan->type) {
            case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
            case duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
                return FindPSTOperator(plan->children[0]);
            }
            case duckdb::LogicalOperatorType::LOGICAL_GET: {
                auto maybe_pst_get = &plan->Cast<LogicalGet>();
                auto maybe_pst_function_mode = duckpst::FUNCTIONS.find(maybe_pst_get->function.name);
                if (maybe_pst_function_mode == duckpst::FUNCTIONS.end()) 
                    return {};
                return maybe_pst_get;
            }
            default:
                return {};
        }
    }

    map<idx_t, unique_ptr<TableFilter>> PSTFilterOptimizer::CollectAndEraseNonVirtualColumnFilters(LogicalGet &pst_logical_get) {
        map<idx_t, unique_ptr<TableFilter>> non_vcol_filters;
        
        for (auto &[schema_col, f] : pst_logical_get.table_filters.filters) {
            if (schema_col >= schema::DUCKDB_VIRTUAL_COLUMN_START) 
                continue;

            non_vcol_filters.insert({ schema_col, f->Copy() });
        }

        for (auto &[schema_col, _] : non_vcol_filters) {
            pst_logical_get.table_filters.filters.erase(schema_col);
        }

        return non_vcol_filters;
    }

    unique_ptr<LogicalProjection> PSTFilterOptimizer::CreateProjection(idx_t new_table_index, LogicalGet &pst_logical_get) {
        auto bound_column_ids = pst_logical_get.GetColumnIds();
        auto column_bindings = pst_logical_get.GetColumnBindings();
        vector<unique_ptr<Expression>> input_column_refs;

        // To make mapping easier, the new projection is 1:1 with the logical get output
        for (idx_t column_id=0; column_id < bound_column_ids.size(); ++column_id) {
            auto column_index = bound_column_ids[column_id];
            input_column_refs.emplace_back(make_uniq<BoundColumnRefExpression>(
                pst_logical_get.GetColumnName(column_index),
                pst_logical_get.GetColumnType(column_index),
                column_bindings[column_id]
            ));
        }

        return std::move(make_uniq<LogicalProjection>(
            new_table_index,
            std::move(input_column_refs)
        ));
    }

    void PSTFilterOptimizer::RewriteEligiblePSTFilters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan, map<idx_t, vector<ColumnBinding>> &projection_rewrites) {
        if (!plan) return;

        for (idx_t i=0; i < plan->children.size(); ++i) {
            auto &child = plan->children[i];

            // Find a PST table function read
            auto maybe_pst_logical_get = FindPSTOperator(child);
            if (!maybe_pst_logical_get) continue;
            auto &pst_logical_get = (*maybe_pst_logical_get)->Cast<LogicalGet>();

            if (child->type != LogicalOperatorType::LOGICAL_PROJECTION) continue;
            auto &original_projection = child->Cast<LogicalProjection>();

            // Collect non virtual column filters, erasing them from the LOGICAL_GET table filters
            auto non_vcol_filters = CollectAndEraseNonVirtualColumnFilters(pst_logical_get);
            if (non_vcol_filters.empty()) continue;

            // Ask the binder for a new table index and create a widened projection
            // we can use in a subsequent filter operator
            auto projection_table_index = input.optimizer.binder.GenerateTableIndex();
            auto new_projection = CreateProjection(projection_table_index, pst_logical_get);
            new_projection->children.emplace_back(pst_logical_get.Copy(input.context));

            // Keep track of the original projection mapping, as we'll need to rewrite any remaining references at the end
            if (projection_rewrites.find(original_projection.table_index) == projection_rewrites.end()) {
                vector<ColumnBinding> get_map;
                for (auto &original_proj_binding : original_projection.GetColumnBindings()) {
                    auto &original_proj_expr = original_projection.expressions[original_proj_binding.column_index];
                    auto &pst_column_id = original_proj_expr->Cast<BoundColumnRefExpression>().binding.column_index;
                    auto new_binding = ColumnBinding(projection_table_index, pst_column_id);
                    get_map.emplace_back(std::move(new_binding));
                }

                projection_rewrites.insert({original_projection.table_index, std::move(get_map)});
            }

            auto logical_filter = make_uniq<LogicalFilter>();
            for (auto &[schema_col, tfilter] : non_vcol_filters) {
                std::optional<idx_t> projection_id;
                for (idx_t pid=0; pid < pst_logical_get.GetColumnIds().size(); ++pid) {
                    if (pst_logical_get.GetColumnIds()[pid].GetPrimaryIndex() == schema_col) {
                        projection_id.emplace(pid);
                        break;
                    }
                }

                if (!projection_id) continue;
                logical_filter->expressions.emplace_back(tfilter->ToExpression(BoundColumnRefExpression(
                    pst_logical_get.names[schema_col],
                    pst_logical_get.returned_types[schema_col],
                    ColumnBinding(new_projection->table_index, *projection_id)
                )));
            }

            logical_filter->AddChild(std::move(new_projection));
            plan->children[i] = std::move(logical_filter);
        }

        for (auto &child : plan->children) {
            RewriteEligiblePSTFilters(input, child, projection_rewrites);
        }
    }

    void PSTFilterOptimizer::UpdateProjectionBindings(unique_ptr<LogicalOperator> &plan, map<idx_t, vector<ColumnBinding>> &projection_rewrites) {
        LogicalOperatorVisitor::EnumerateExpressions(*plan, [&](unique_ptr<Expression> *expr) {
            if (!expr) return;
            if ((*expr)->type != ExpressionType::BOUND_COLUMN_REF) return;

            auto &bound_col_ref = (*expr)->Cast<BoundColumnRefExpression>();
            auto maybe_rewrite = projection_rewrites.find(bound_col_ref.binding.table_index);

            if (maybe_rewrite == projection_rewrites.end()) return;
            bound_col_ref.binding = maybe_rewrite->second.at(bound_col_ref.binding.column_index);
        });

        for (auto &child_plan : plan->children) {
            UpdateProjectionBindings(child_plan, projection_rewrites);
        }
    }

    void PSTFilterOptimizer::EnsureNonVirtualColumnFilters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
        map<idx_t, vector<ColumnBinding>> projection_rewrites;
        RewriteEligiblePSTFilters(input, plan, projection_rewrites);

        if (projection_rewrites.empty()) return;
        UpdateProjectionBindings(plan, projection_rewrites);

        printf("%s", plan->ToString().data());
    }

    PSTFilterOptimizer::PSTFilterOptimizer() {
        optimize_function = EnsureNonVirtualColumnFilters;
    }
}

#include "storage/bigquery_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "bigquery_scanner.hpp"

namespace duckdb {

static bool IsBigQueryScan(const string &function_name) {
	return function_name == "bigquery_scan";
}

// Function to optimize BigQuery scans by handling limit and offset at the scan level.
void OptimizeBigQueryScan(unique_ptr<LogicalOperator> &op) {
    // Check if the operator is a LIMIT operator
    if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
        // Cast the operator to a LogicalLimit for further processing
        auto &limit = op->Cast<LogicalLimit>();
        // Get the first child of the limit operator, which is the target of optimization
        reference<LogicalOperator> child = *op->children[0];
        // Traverse down the operator tree until a non-PROJECTION operator is found
        while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
            child = *child.get().children[0];
        }
        // If the final operator is not a GET operator, exit the function as no optimization can be applied
        if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
            return;
        }
        // Cast the operator to LogicalGet for further processing
        auto &get = child.get().Cast<LogicalGet>();
        // Check if the target of the GET operator is a BigQuery scan
        if (!IsBigQueryScan(get.function.name)) {
            return;
        }
        // Validate that the limit value is either a constant or unset; otherwise, exit
        switch (limit.limit_val.Type()) {
        case LimitNodeType::CONSTANT_VALUE:
        case LimitNodeType::UNSET:
            break;
        default:
            // If the limit is not a constant or unset, optimization cannot be applied
            return;
        }
        // Validate that the offset value is either a constant or unset; otherwise, exit
        switch (limit.offset_val.Type()) {
        case LimitNodeType::CONSTANT_VALUE:
        case LimitNodeType::UNSET:
            break;
        default:
            // If the offset is not a constant or unset, optimization cannot be applied
            return;
        }
        // Cast the bind data of the GET operator to BigQueryScanBindData for modification
        auto &bind_data = get.bind_data->Cast<BigQueryScanBindData>();
		//Printer::Print("OptimizeBigQueryScan");
        // If a limit is set, apply it to the bind data
        if (limit.limit_val.Type() != LimitNodeType::UNSET) {
            bind_data.limit = limit.limit_val.GetConstantValue();
            bind_data.has_limit = true;
        }
        // If an offset is set, apply it to the bind data
        if (limit.offset_val.Type() != LimitNodeType::UNSET) {
            bind_data.offset = limit.offset_val.GetConstantValue();
        }
        // Remove the limit operator from the tree, effectively applying the limit at the scan level
        op = std::move(op->children[0]);
        return;
    }
    // If the current operator is not a LIMIT, recursively apply this optimization to all children
    for (auto &child : op->children) {
        OptimizeBigQueryScan(child);
    }
}

void BigQueryOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan){
	 OptimizeBigQueryScan(plan);
}

} // namespace duckdb

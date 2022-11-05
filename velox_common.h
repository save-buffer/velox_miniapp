#pragma once

#include <google/protobuf/util/json_util.h>
#include <folly/init/Init.h>

#include <velox/exec/Task.h>
#include <velox/core/PlanFragment.h>
#include <velox/core/QueryCtx.h>
#include <velox/common/memory/Memory.h>
#include <velox/substrait/SubstraitToVeloxPlan.h>
#include <velox/common/file/FileSystems.h>
#include <velox/connectors/hive/HiveConnector.h>
#include <velox/connectors/hive/HiveConnectorSplit.h>
#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/functions/prestosql/registration/RegistrationFunctions.h>

// It's only like this because of Folly, I swear!
void InitVelox(int *argc, char ***argv)
{
    folly::init(argc, argv);

    // Zeal of wexort! Galeron's Abyssal Carnesphere! Zuckerberg's Factory of Hive Connectors! - Carl, the Invoker
    auto hive_connector = facebook::velox::connector::getConnectorFactory(
        facebook::velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)->newConnector("test-hive", nullptr);
    facebook::velox::connector::registerConnector(hive_connector);
    facebook::velox::filesystems::registerLocalFileSystem();
    facebook::velox::parquet::registerParquetReaderFactory();
    facebook::velox::functions::prestosql::registerAllScalarFunctions();
}

std::shared_ptr<facebook::velox::exec::Task> ExecuteSubstrait(substrait::Plan &substrait_plan, bool print_plan = false)
{
    std::shared_ptr<facebook::velox::core::QueryCtx> ctx = facebook::velox::core::QueryCtx::createForTest();
    facebook::velox::substrait::SubstraitVeloxPlanConverter converter(ctx->pool());
    std::shared_ptr<const facebook::velox::core::PlanNode> plan_node = converter.toVeloxPlan(substrait_plan);
    if(print_plan)
        std::cout << "Plan:\n" << plan_node->toString(true, true) << "\n";
    facebook::velox::core::PlanFragment fragment(std::move(plan_node));
    auto task = std::make_shared<facebook::velox::exec::Task>("plan", fragment, /*destination partition=*/0, ctx);
    for(const auto &split_info : converter.splitInfos())
    {
        for(int i = 0; i < split_info.second->paths.size(); i++)
        {
            auto connector_split = std::make_shared<facebook::velox::connector::hive::HiveConnectorSplit>(
                "test-hive" /*connectorId*/,
                split_info.second->paths[i],
                split_info.second->format,
                split_info.second->starts[i],
                split_info.second->lengths[i]);
            facebook::velox::exec::Split split;
            split.connectorSplit = std::move(connector_split);
            task->addSplit(split_info.first, std::move(split));
        }
        task->noMoreSplits(split_info.first);
    }
    return task;
}

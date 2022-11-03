#include <iostream>
#include <fstream>
#include <string_view>

#include <google/protobuf/util/json_util.h>

#include <folly/init/Init.h>
#include "velox/exec/Task.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/common/memory/Memory.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

constexpr const char *Usage =
    "\tfrom_substrait <filename>\n"
    "\tFile type is inferred from extension. If it ends in .pb or .proto, it's a protobuf.\n"
    "\tIf it ends in .json, it's a json. If neither, it's an error.\n";

// In C++20 we'd just use string_view::ends_with...
bool MatchesExtension(std::string_view filename, std::string_view extension)
{
    if(extension.size() > filename.size())
        return false;
    return std::equal(extension.rbegin(), extension.rend(), filename.rbegin());
}

int main(int argc, char **argv)
{
    folly::init(&argc, &argv);

    if(argc != 2)
    {
        std::cerr << "Usage: " << Usage;
        return 1;
    }

    std::string_view filename(argv[1]);
    bool is_json = MatchesExtension(filename, ".json");
    bool is_pb = MatchesExtension(filename, ".pb") || MatchesExtension(filename, ".proto");
 
    if(!is_json && !is_pb)
    {
        std::cerr << "Unrecognized file extension for file " << filename << '\n';
        return 1;
    }

    substrait::Plan substrait_plan;

    if(is_json)
    {
        std::ifstream stream(argv[1]);
        std::stringstream json;
        json << stream.rdbuf();
        auto status = google::protobuf::util::JsonStringToMessage(json.str(), &substrait_plan);
        if(!status.ok())
        {
            std::cerr << "Failed to parse JSON from file " << filename << " as json: " << status.message() << '\n';
            return 1;
        }
    }
    else if (is_pb)
    {
        std::ifstream stream(argv[1], std::ios::binary);
        if(!substrait_plan.ParseFromIstream(&stream))
        {
            std::cerr << "Failed to parse a substrait plan from the given file: " << argv[1] << '\n';
            return 1;
        }
    }

    std::shared_ptr<facebook::velox::core::QueryCtx> ctx = facebook::velox::core::QueryCtx::createForTest();
    facebook::velox::substrait::SubstraitVeloxPlanConverter converter(ctx->pool());
    std::shared_ptr<const facebook::velox::core::PlanNode> plan_node = converter.toVeloxPlan(substrait_plan);
    std::cout << "Plan:\n" << plan_node->toString(true, true) << "\n";
    facebook::velox::core::PlanFragment fragment(std::move(plan_node));

    // Zeal of wexort! Galeron's Abyssal Carnesphere! Zuckerberg's Factory of Hive Connectors! - Carl, the Invoker
    auto hive_connector = facebook::velox::connector::getConnectorFactory(
        facebook::velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)->newConnector("test-hive", nullptr);
    facebook::velox::connector::registerConnector(hive_connector);
    facebook::velox::filesystems::registerLocalFileSystem();
    facebook::velox::parquet::registerParquetReaderFactory();
    facebook::velox::functions::prestosql::registerAllScalarFunctions();

    auto task = std::make_shared<facebook::velox::exec::Task>("plan", fragment, /*destination partition=*/0, ctx);
    for(const auto &split_info : converter.splitInfos())
    {
        for(int i = 0; i < split_info.second->paths.size(); i++)
        {
            std::cout << split_info.second->paths[i] << ", " << split_info.second->starts[i] << ", " << split_info.second->lengths[i] << "\n";
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

    int num_batches_received = 0;
    while(auto result = task->next())
    {
        std::cout << "Got a result of size " << result->size() << ":\n";
        for(auto i = 0; i < result->size(); i++)
            std::cout << "\t" << result->toString(i) << "\n";
        num_batches_received++;
    }
    std::cout << "Finished with " << num_batches_received << " batches received\n";
    return 0;
}

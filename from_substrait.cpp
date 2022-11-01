#include <iostream>
#include <fstream>

#include <folly/init/Init.h>
#include "velox/exec/Task.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/common/memory/Memory.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"

int main(int argc, char **argv)
{
    folly::init(&argc, &argv);

    substrait::Plan substrait_plan;
    {
        std::ifstream stream(argv[1], std::ios::binary);
        if(!substrait_plan.ParseFromIstream(&stream))
        {
            std::cerr << "Failed to parse a substrait plan from the given file: " << argv[1] << '\n';
            return 1;
        }
    }

    auto ctx = std::make_shared<facebook::velox::core::QueryCtx>();
    facebook::velox::substrait::SubstraitVeloxPlanConverter converter(ctx->pool());
    auto plan_node = converter.toVeloxPlan(substrait_plan);
    facebook::velox::core::PlanFragment fragment(std::move(plan_node));

    facebook::velox::exec::Task task("plan", fragment, /*destination partition=*/0, ctx);
    while(auto result = task.next())
    {
        for(auto i = 0; i < result->size(); i++)
            std::cout << result->toString(i);
    }
    return 0;
}

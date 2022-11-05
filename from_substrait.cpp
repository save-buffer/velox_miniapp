#include <iostream>
#include <fstream>
#include <string_view>

#include "velox_common.h"

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
    if(argc != 2)
    {
        std::cerr << "Usage: " << Usage;
        return 1;
    }

    InitVelox(&argc, &argv);

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

    auto task = ExecuteSubstrait(substrait_plan, true /*print_plan*/);

    uint64_t num_batches_received = 0;
    uint64_t num_rows_received = 0;
    while(auto result = task->next())
    {
        std::cout << "Got a result of size " << result->size() << ":\n";
        for(auto i = 0; i < result->size(); i++)
            std::cout << "\t" << result->toString(i) << "\n";
        num_batches_received++;
        num_rows_received += result->size();
    }
    std::cout << "Finished with " << num_batches_received << " batches, " << num_rows_received << " rows received\n";
    return 0;
}

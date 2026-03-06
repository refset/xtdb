#include "loader.h"
#include "queries.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <set>

using json = nlohmann::json;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: bespoke_olap <catalog.json>\n";
        return 1;
    }

    auto cat = parse_catalog(argv[1]);
    std::cerr << "Params: system_id=" << cat.params.sample_system_id
              << " min_vt=" << cat.params.min_valid_time
              << " max_vt=" << cat.params.max_valid_time << "\n";

    struct QueryDef {
        std::string name;
        std::set<std::string> tables;
        std::function<json(const FusionData&, const QueryParams&)> fn;
    };
    std::vector<QueryDef> queries = {
        {"system-settings", {"system"}, query_system_settings},
        {"readings-for-system", {"system", "readings"}, query_readings_for_system},
        {"system-count-over-time", {"system"}, query_system_count_over_time},
        {"readings-range-bins", {"readings"}, query_readings_range_bins},
        {"cumulative-registration", {"system", "site", "device", "test_suite_run", "test_suite", "test_case", "test_case_run"}, query_cumulative_registration}
    };

    auto median_of = [](std::vector<int64_t>& v) -> int64_t {
        std::sort(v.begin(), v.end());
        return v[v.size() / 2];
    };

    std::cerr << "Running queries (3 iterations each, load+compute, reporting median)...\n";
    json output;
    json query_arr = json::array();

    for (auto& qd : queries) {
        std::vector<int64_t> timings;
        json last_results;
        int64_t last_rows = 0;
        for (int iter = 0; iter < 3; iter++) {
            auto start = std::chrono::high_resolution_clock::now();
            auto data = load_tables(cat, qd.tables);
            last_results = qd.fn(data, cat.params);
            auto end = std::chrono::high_resolution_clock::now();
            timings.push_back(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
            last_rows = last_results.is_array() ? static_cast<int64_t>(last_results.size()) : 0;
        }
        auto med = median_of(timings);
        std::cerr << "  " << qd.name << ": " << last_rows << " rows, median " << med << " us"
                  << " [" << timings[0] << ", " << timings[1] << ", " << timings[2] << "]\n";
        query_arr.push_back({
            {"query", qd.name},
            {"latency_us", med},
            {"row_count", last_rows},
            {"results", last_results}
        });
    }

    output["queries"] = query_arr;
    std::cout << output.dump() << std::endl;
    return 0;
}

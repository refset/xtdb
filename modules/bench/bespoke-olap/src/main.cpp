#include "loader.h"
#include "queries.h"

#include <chrono>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct QueryResult {
    std::string name;
    json results;
    int64_t latency_us;
    int64_t row_count;
};

template<typename Fn>
static QueryResult run_query(const std::string& name, Fn fn) {
    auto start = std::chrono::high_resolution_clock::now();
    json results = fn();
    auto end = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    int64_t rows = results.is_array() ? static_cast<int64_t>(results.size()) : 0;
    std::cerr << "  " << name << ": " << rows << " rows in " << us << " us\n";

    return {name, std::move(results), us, rows};
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: bespoke_olap <catalog.json>\n";
        return 1;
    }

    std::string catalog_path = argv[1];
    QueryParams params;

    std::cerr << "Loading data from catalog: " << catalog_path << "\n";
    auto load_start = std::chrono::high_resolution_clock::now();
    FusionData data = load_from_catalog(catalog_path, params);
    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_us = std::chrono::duration_cast<std::chrono::microseconds>(load_end - load_start).count();
    std::cerr << "Data loaded in " << load_us << " us\n";
    std::cerr << "Params: system_id=" << params.sample_system_id
              << " min_vt=" << params.min_valid_time
              << " max_vt=" << params.max_valid_time << "\n";

    std::cerr << "Running queries...\n";
    std::vector<QueryResult> results;

    results.push_back(run_query("system-settings",
        [&]() { return query_system_settings(data, params); }));

    results.push_back(run_query("readings-for-system",
        [&]() { return query_readings_for_system(data, params); }));

    results.push_back(run_query("system-count-over-time",
        [&]() { return query_system_count_over_time(data, params); }));

    results.push_back(run_query("readings-range-bins",
        [&]() { return query_readings_range_bins(data, params); }));

    results.push_back(run_query("cumulative-registration",
        [&]() { return query_cumulative_registration(data, params); }));

    // Output JSON to stdout
    json output;
    output["load_us"] = load_us;
    json queries = json::array();
    for (auto& r : results) {
        queries.push_back({
            {"query", r.name},
            {"latency_us", r.latency_us},
            {"row_count", r.row_count},
            {"results", r.results}
        });
    }
    output["queries"] = queries;

    std::cout << output.dump() << std::endl;
    return 0;
}

#include "queries.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <unordered_set>

static constexpr int64_t HOUR_US = 3600LL * 1000000LL;
static constexpr int64_t FORTY_EIGHT_HOURS_US = 48LL * 3600LL * 1000000LL;

static int64_t date_bin_hour(int64_t ts) {
    if (ts >= 0) return (ts / HOUR_US) * HOUR_US;
    return ((ts - HOUR_US + 1) / HOUR_US) * HOUR_US;
}

// ---------------------------------------------------------------------------
// Q1: system-settings — O(1) lookup
// ---------------------------------------------------------------------------

json query_system_settings(const FusionData& data, const QueryParams& params) {
    auto it = data.system.id_latest.find(params.sample_system_id);
    if (it == data.system.id_latest.end()) return json::array();

    int32_t idx = it->second;
    json row;
    row["_id"] = data.system.id[idx];
    row["_valid_from"] = data.system.valid_from[idx];
    row["_valid_to"] = data.system.valid_to[idx];
    row["site_id"] = data.system.site_id[idx];
    row["created_at"] = data.system.created_at[idx];
    row["type"] = data.system.type[idx];
    row["updated_time"] = data.system.updated_time[idx];
    row["rtg_max_w"] = data.system.rtg_max_w[idx];
    row["rtg_max_wh"] = data.system.rtg_max_wh[idx];
    row["rtg_max_va"] = data.system.rtg_max_va[idx];
    row["rtg_max_var"] = data.system.rtg_max_var[idx];
    row["rtg_max_var_neg"] = data.system.rtg_max_var_neg[idx];
    row["rtg_max_a"] = data.system.rtg_max_a[idx];
    row["rtg_max_v"] = data.system.rtg_max_v[idx];
    row["rtg_min_v"] = data.system.rtg_min_v[idx];
    row["rtg_v_nom"] = data.system.rtg_v_nom[idx];
    row["rtg_max_charge_rate_w"] = data.system.rtg_max_charge_rate_w[idx];
    row["rtg_max_charge_rate_va"] = data.system.rtg_max_charge_rate_va[idx];
    row["rtg_max_discharge_rate_w"] = data.system.rtg_max_discharge_rate_w[idx];
    row["rtg_max_discharge_rate_va"] = data.system.rtg_max_discharge_rate_va[idx];
    row["rtg_min_pf_over_excited"] = data.system.rtg_min_pf_over_excited[idx];
    row["rtg_min_pf_under_excited"] = data.system.rtg_min_pf_under_excited[idx];
    row["set_max_w"] = data.system.set_max_w[idx];
    row["set_max_wh"] = data.system.set_max_wh[idx];
    row["set_max_va"] = data.system.set_max_va[idx];
    row["set_max_var"] = data.system.set_max_var[idx];
    row["set_max_var_neg"] = data.system.set_max_var_neg[idx];
    row["set_max_charge_rate_w"] = data.system.set_max_charge_rate_w[idx];
    row["set_max_discharge_rate_w"] = data.system.set_max_discharge_rate_w[idx];
    row["set_grad_w"] = data.system.set_grad_w[idx];
    row["modes_enabled"] = data.system.modes_enabled[idx];
    row["modes_supported"] = data.system.modes_supported[idx];

    return json::array({row});
}

// ---------------------------------------------------------------------------
// Q2: readings-for-system — cartesian product (matches buggy SQL)
// ---------------------------------------------------------------------------

json query_readings_for_system(const FusionData& data, const QueryParams& params) {
    const auto& rdg = data.readings;
    const auto& sys = data.system;

    auto range_it = rdg.system_id_range.find(params.sample_system_id);
    if (range_it == rdg.system_id_range.end()) return json::array();

    int32_t rstart = range_it->second.first;
    int32_t rend = range_it->second.second;

    auto versions_it = sys.id_versions.find(params.sample_system_id);
    if (versions_it == sys.id_versions.end()) return json::array();
    const auto& sys_versions = versions_it->second;

    // Find readings in [min_valid_time, max_valid_time) using lower_bound
    int32_t lo = rstart;
    while (lo < rend && rdg.valid_from[lo] < params.min_valid_time) lo++;
    int32_t hi = lo;
    while (hi < rend && rdg.valid_from[hi] < params.max_valid_time) hi++;

    // Cartesian product: each reading × each system version (matches SQL bug)
    struct ResultRow { int64_t reading_time; double value; };
    std::vector<ResultRow> results;
    results.reserve(static_cast<size_t>(hi - lo) * sys_versions.size());

    for (int32_t ri = lo; ri < hi; ri++) {
        for (auto si : sys_versions) {
            results.push_back({rdg.valid_to[ri], rdg.value[ri]});
        }
    }

    std::sort(results.begin(), results.end(),
              [](const ResultRow& a, const ResultRow& b) {
                  return a.reading_time < b.reading_time;
              });

    json arr = json::array();
    for (auto& r : results) {
        arr.push_back({{"reading_time", r.reading_time}, {"reading_value", r.value}});
    }
    return arr;
}

// ---------------------------------------------------------------------------
// Q3: system-count-over-time — join elimination optimization
// The 6 LEFT OUTER JOINs are semantically irrelevant to COUNT(DISTINCT system._id)
// since a system counts regardless of join success. We just count systems valid
// at each hourly timestamp.
// ---------------------------------------------------------------------------

json query_system_count_over_time(const FusionData& data, const QueryParams& params) {
    int64_t start = date_bin_hour(params.min_valid_time);
    int64_t end = params.max_valid_time;

    // Collect all (valid_from, valid_to) intervals per unique system id
    // For count distinct, we just need to know if *any* version is valid at time t
    struct Interval { int64_t from; int64_t to; };
    std::vector<Interval> intervals;
    for (auto& [id, versions] : data.system.id_versions) {
        for (auto idx : versions) {
            intervals.push_back({data.system.valid_from[idx], data.system.valid_to[idx]});
        }
    }

    // Sweep-line: for each hourly timestamp, count distinct systems
    // Since we need DISTINCT system._id, we need per-id tracking
    // Approach: for each hour t, iterate systems and check if any version contains t
    json arr = json::array();
    for (int64_t t = start; t <= end; t += HOUR_US) {
        int64_t count = 0;
        for (auto& [id, versions] : data.system.id_versions) {
            for (auto idx : versions) {
                if (data.system.valid_from[idx] <= t && t < data.system.valid_to[idx]) {
                    count++;
                    break;
                }
            }
        }
        arr.push_back({{"d", t}, {"c", count}});
    }
    return arr;
}

// ---------------------------------------------------------------------------
// Q4: readings-range-bins — hourly weighted aggregation
// Replicates: range_bins(INTERVAL 'PT1H', reading._valid_time)
// For each reading, compute bin overlaps with hourly buckets, accumulate
// weighted portion and weight per bin.
// ---------------------------------------------------------------------------

json query_readings_range_bins(const FusionData& data, const QueryParams& params) {
    const auto& rdg = data.readings;
    int64_t query_start = params.min_valid_time;
    int64_t query_end = params.max_valid_time;

    // Use an ordered map: bin_start → (sum_portion, sum_weight)
    std::map<int64_t, std::pair<double, double>> bins;

    for (int32_t i = 0; i < static_cast<int32_t>(rdg.valid_from.size()); i++) {
        int64_t vf = rdg.valid_from[i];
        int64_t vt = rdg.valid_to[i];
        double val = rdg.value[i];

        if (vf >= query_end || vt <= query_start) continue;
        if (vf >= query_end) continue;

        // range_bins: split [vf, vt) into hourly bins
        int64_t reading_duration = vt - vf;
        if (reading_duration <= 0) continue;

        int64_t bin_start = date_bin_hour(vf);
        while (bin_start < vt) {
            int64_t bin_end = bin_start + HOUR_US;
            int64_t overlap_start = std::max(vf, bin_start);
            int64_t overlap_end = std::min(vt, bin_end);
            if (overlap_start < overlap_end) {
                double weight = static_cast<double>(overlap_end - overlap_start)
                              / static_cast<double>(reading_duration);
                double portion = val * weight;
                auto& b = bins[bin_start];
                b.first += portion;
                b.second += weight;
            }
            bin_start += HOUR_US;
        }
    }

    json arr = json::array();
    for (auto& [t, pw] : bins) {
        double value = (pw.second > 0.0) ? pw.first / pw.second : 0.0;
        arr.push_back({{"t", t}, {"value", value}});
    }
    return arr;
}

// ---------------------------------------------------------------------------
// Q5: cumulative-registration — complex multi-CTE query
//
// For each hourly timestamp t:
//   For each system valid at t:
//     - site_linked: site._id IS NOT NULL (system.site_id exists in site table)
//     - devices_linked: COUNT(device._id) >= 1 (any device for this system valid at t)
//     - test_suite_run_ok: latest test_suite_run.status = 'DONE'
//     - expected_test_cases: count of test_cases for the test_suite
//     - passing_test_cases: count of test_case_runs with status='OK'
//     - status = CASE WHEN all conditions → 'Success'
//                     WHEN created_at + 48h < t → 'Failed'
//                     ELSE 'Pending'
//   GROUP BY (t, status), COUNT
// ---------------------------------------------------------------------------

json query_cumulative_registration(const FusionData& data, const QueryParams& params) {
    int64_t start = date_bin_hour(params.min_valid_time);
    int64_t end = params.max_valid_time;

    const auto& sys = data.system;
    const auto& site = data.site;
    const auto& dev = data.device;
    const auto& tsr = data.test_suite_run;
    const auto& ts = data.test_suite;
    const auto& tc = data.test_case;
    const auto& tcr = data.test_case_run;

    // Pre-compute: for each test_suite_id, count of test_cases
    std::unordered_map<std::string, int64_t> test_case_count;
    for (int32_t i = 0; i < static_cast<int32_t>(tc.id.size()); i++) {
        test_case_count[tc.test_suite_id[i]]++;
    }

    // Ordered map: (t, status) → count
    std::map<std::pair<int64_t, std::string>, int64_t> result_map;

    for (int64_t t = start; t <= end; t += HOUR_US) {
        // For each system valid at t
        for (auto& [sys_id, versions] : sys.id_versions) {
            // Find a version valid at t
            int32_t active_idx = -1;
            for (auto idx : versions) {
                if (sys.valid_from[idx] <= t && t < sys.valid_to[idx]) {
                    active_idx = idx;
                    break;
                }
            }
            if (active_idx < 0) continue;

            // site_linked
            bool site_linked = site.id_index.count(sys.site_id[active_idx]) > 0;

            // devices_linked: any device for this system valid at t
            bool devices_linked = false;
            auto dev_it = dev.system_id_index.find(sys_id);
            if (dev_it != dev.system_id_index.end()) {
                for (auto di : dev_it->second) {
                    if (dev.valid_from[di] <= t && t < dev.valid_to[di]) {
                        devices_linked = true;
                        break;
                    }
                }
            }

            // latest_test_suite_run: find latest TSR for this system valid at t
            // (sorted by valid_from DESC in system_id_index)
            int32_t latest_tsr_idx = -1;
            auto tsr_it = tsr.system_id_index.find(sys_id);
            if (tsr_it != tsr.system_id_index.end()) {
                for (auto ti : tsr_it->second) {
                    if (tsr.valid_from[ti] <= t && t < tsr.valid_to[ti]) {
                        // Also need: test_suite must be valid at t
                        auto ts_it = ts.id_index.find(tsr.test_suite_id[ti]);
                        if (ts_it != ts.id_index.end()) {
                            latest_tsr_idx = ti;
                            break;
                        }
                    }
                }
            }

            bool test_ok = false;
            int64_t expected = 0;
            int64_t passing = 0;

            if (latest_tsr_idx >= 0) {
                test_ok = (tsr.status[latest_tsr_idx] == "DONE");

                // expected_test_cases: count test_cases for the test_suite
                auto tc_count_it = test_case_count.find(tsr.test_suite_id[latest_tsr_idx]);
                if (tc_count_it != test_case_count.end()) {
                    expected = tc_count_it->second;
                }

                // passing_test_cases: count test_case_runs for this TSR with status='OK'
                // and valid at t
                auto tcr_it = tcr.suite_run_id_index.find(tsr.id[latest_tsr_idx]);
                if (tcr_it != tcr.suite_run_id_index.end()) {
                    for (auto ci : tcr_it->second) {
                        if (tcr.valid_from[ci] <= t && t < tcr.valid_to[ci] &&
                            tcr.status[ci] == "OK") {
                            passing++;
                        }
                    }
                }
            }

            // CASE logic
            std::string status;
            if (site_linked && devices_linked && test_ok && expected == passing) {
                status = "Success";
            } else if (sys.created_at[active_idx] + FORTY_EIGHT_HOURS_US < t) {
                status = "Failed";
            } else {
                status = "Pending";
            }

            result_map[{t, status}]++;
        }
    }

    json arr = json::array();
    for (auto& [key, count] : result_map) {
        arr.push_back({{"t", key.first},
                        {"registration_status", key.second},
                        {"c", count}});
    }

    return arr;
}

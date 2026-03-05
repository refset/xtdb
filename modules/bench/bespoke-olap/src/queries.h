#pragma once

#include "types.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

json query_system_settings(const FusionData& data, const QueryParams& params);
json query_readings_for_system(const FusionData& data, const QueryParams& params);
json query_system_count_over_time(const FusionData& data, const QueryParams& params);
json query_readings_range_bins(const FusionData& data, const QueryParams& params);
json query_cumulative_registration(const FusionData& data, const QueryParams& params);

#pragma once

#include "arrow_access.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

using Timestamp = int64_t; // microseconds since epoch

struct Organisation {
    std::unordered_map<std::string, int32_t> id_index;
    std::vector<std::string> id;
    std::vector<std::string> name;
};

struct DeviceSeries {
    std::unordered_map<std::string, int32_t> id_index;
    std::vector<std::string> id;
    std::vector<std::string> organisation_id;
    std::vector<std::string> name;
};

struct DeviceModel {
    std::unordered_map<std::string, int32_t> id_index;
    std::vector<std::string> id;
    std::vector<std::string> device_series_id;
    std::vector<std::string> name;
    std::vector<double> capacity_kw;
};

struct Site {
    std::unordered_map<std::string, int32_t> id_index;
    std::vector<std::string> id;
    std::vector<std::string> address;
    std::vector<std::string> postcode;
    std::vector<std::string> state;
};

struct System {
    std::unordered_map<std::string, std::vector<int32_t>> id_versions;
    std::unordered_map<std::string, int32_t> id_latest;

    std::vector<std::string> id;
    std::vector<Timestamp> valid_from;
    std::vector<Timestamp> valid_to;
    std::vector<std::string> site_id;
    std::vector<Timestamp> created_at;

    // Zero-copy: other columns accessed from Arrow on demand
    std::vector<FileRef> files;
    std::vector<RowSource> sources;
};

struct Device {
    std::unordered_map<std::string, std::vector<int32_t>> system_id_index;
    std::vector<std::string> id;
    std::vector<std::string> system_id;
    std::vector<std::string> device_model_id;
    std::vector<Timestamp> valid_from;
    std::vector<Timestamp> valid_to;
};

struct Readings {
    std::unordered_map<std::string, std::pair<int32_t, int32_t>> system_id_range;
    std::vector<Timestamp> valid_from;
    std::vector<Timestamp> valid_to;
    std::vector<double> value;
};

struct TestSuite {
    std::unordered_map<std::string, int32_t> id_index;
    std::vector<std::string> id;
    std::vector<std::string> purpose;
    std::vector<std::string> name;
};

struct TestCase {
    std::unordered_map<std::string, int32_t> id_index;
    std::vector<std::string> id;
    std::vector<std::string> test_suite_id;
    std::vector<std::string> name;
};

struct TestSuiteRun {
    std::unordered_map<std::string, int32_t> id_index;
    std::unordered_map<std::string, std::vector<int32_t>> system_id_index;
    std::vector<std::string> id;
    std::vector<std::string> system_id;
    std::vector<std::string> test_suite_id;
    std::vector<std::string> status;
    std::vector<Timestamp> started_at;
    std::vector<Timestamp> completed_at;
    std::vector<Timestamp> valid_from;
    std::vector<Timestamp> valid_to;
};

struct TestCaseRun {
    std::unordered_map<std::string, std::vector<int32_t>> suite_run_id_index;
    std::vector<std::string> id;
    std::vector<std::string> test_suite_run_id;
    std::vector<std::string> test_case_id;
    std::vector<std::string> status;
    std::vector<Timestamp> executed_at;
    std::vector<Timestamp> valid_from;
    std::vector<Timestamp> valid_to;
};

struct FusionData {
    Organisation organisation;
    DeviceSeries device_series;
    DeviceModel device_model;
    Site site;
    System system;
    Device device;
    Readings readings;
    TestSuite test_suite;
    TestCase test_case;
    TestSuiteRun test_suite_run;
    TestCaseRun test_case_run;
};

struct QueryParams {
    std::string sample_system_id;
    Timestamp min_valid_time;
    Timestamp max_valid_time;
};

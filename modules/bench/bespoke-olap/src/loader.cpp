#include "loader.h"
#include "bitemporal.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string_view>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// Raw pointer helpers — extract typed array pointers once per batch,
// then use direct pointer arithmetic per row (no type checks, no hash maps)
// ---------------------------------------------------------------------------

static const int64_t* raw_i64(const std::shared_ptr<arrow::Array>& a) {
    if (!a) return nullptr;
    switch (a->type_id()) {
        case arrow::Type::TIMESTAMP:
            return std::static_pointer_cast<arrow::TimestampArray>(a)->raw_values();
        case arrow::Type::INT64:
            return std::static_pointer_cast<arrow::Int64Array>(a)->raw_values();
        default: return nullptr;
    }
}

static const double* raw_f64(const std::shared_ptr<arrow::Array>& a) {
    if (!a) return nullptr;
    if (a->type_id() == arrow::Type::DOUBLE)
        return std::static_pointer_cast<arrow::DoubleArray>(a)->raw_values();
    return nullptr;
}

static IID read_iid(const arrow::FixedSizeBinaryArray* arr, int64_t i) {
    const uint8_t* p = arr->GetValue(i);
    uint64_t high = 0, low = 0;
    for (int b = 0; b < 8; b++) high = (high << 8) | p[b];
    for (int b = 0; b < 8; b++) low = (low << 8) | p[8 + b];
    return {high, low};
}

// ---------------------------------------------------------------------------
// Read all record batches from an Arrow IPC file
// ---------------------------------------------------------------------------

static ArrowFile read_arrow_file(const std::string& path) {
    ArrowFile af;
    auto r = arrow::io::ReadableFile::Open(path);
    if (!r.ok()) return af;
    auto reader_r = arrow::ipc::RecordBatchFileReader::Open(r.ValueOrDie());
    if (!reader_r.ok()) return af;
    auto reader = reader_r.ValueOrDie();
    for (int i = 0; i < reader->num_record_batches(); i++) {
        auto br = reader->ReadRecordBatch(i);
        if (br.ok()) af.batches.push_back(br.ValueOrDie());
    }
    return af;
}

// ---------------------------------------------------------------------------
// Extract events from trie files and resolve via polygon calculation
// Uses typed raw pointers per batch — no per-row type checks.
// ---------------------------------------------------------------------------

static std::vector<ResolvedRow>
resolve_table(std::vector<FileRef>& files, const std::string& table_name = "") {
    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<Event> events;
    int64_t total_rows = 0;
    for (int fi = 0; fi < static_cast<int>(files.size()); fi++) {
        auto& af = files[fi].arrow_file;
        for (int bi = 0; bi < static_cast<int>(af.batches.size()); bi++) {
            auto& batch = af.batches[bi];
            int64_t nr = batch->num_rows();
            total_rows += nr;

            auto iid_col = col(batch, "_iid");
            auto iid_arr = iid_col ? std::static_pointer_cast<arrow::FixedSizeBinaryArray>(iid_col).get() : nullptr;
            const int64_t* sf = raw_i64(col(batch, "_system_from"));
            const int64_t* vf = raw_i64(col(batch, "_valid_from"));
            const int64_t* vt = raw_i64(col(batch, "_valid_to"));

            auto op_col = col(batch, "op");
            const int8_t* type_codes = nullptr;
            if (op_col && op_col->type_id() == arrow::Type::DENSE_UNION)
                type_codes = std::static_pointer_cast<arrow::DenseUnionArray>(op_col)->raw_type_codes();

            for (int64_t r = 0; r < nr; r++) {
                events.push_back({
                    iid_arr ? read_iid(iid_arr, r) : IID{0, 0},
                    sf ? sf[r] : 0,
                    vf ? vf[r] : 0,
                    vt ? vt[r] : 0,
                    type_codes ? (type_codes[r] == 0) : true,
                    fi,
                    static_cast<int32_t>(r),
                    bi
                });
            }
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();

    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
        if (a.iid != b.iid) return a.iid < b.iid;
        return a.system_from > b.system_from;
    });

    auto t2 = std::chrono::high_resolution_clock::now();

    std::vector<ResolvedRow> resolved;
    Ceiling ceiling;
    Polygon polygon;
    IID current_iid = {0, 0};
    bool skip_iid = false;
    bool first = true;

    for (auto& ev : events) {
        if (first || ev.iid != current_iid) {
            ceiling.reset();
            current_iid = ev.iid;
            skip_iid = false;
            first = false;
        }
        if (skip_iid) continue;
        if (!ev.is_put) {
            ceiling.apply_log(ev.system_from, ev.valid_from, ev.valid_to);
            skip_iid = true;
            continue;
        }
        polygon.calculate_for(ceiling, ev.valid_from, ev.valid_to);
        ceiling.apply_log(ev.system_from, ev.valid_from, ev.valid_to);
        for (auto& pr : polygon.ranges) {
            if (pr.valid_from != pr.valid_to && pr.system_to == INT64_MAX) {
                resolved.push_back({
                    pr.valid_from, pr.valid_to,
                    ev.source_file, ev.source_row, ev.source_batch
                });
            }
        }
    }

    auto t3 = std::chrono::high_resolution_clock::now();

    if (!table_name.empty()) {
        auto us = [](auto a, auto b) { return std::chrono::duration_cast<std::chrono::microseconds>(b - a).count(); };
        std::cerr << "    resolve " << table_name << ": " << total_rows << " raw -> "
                  << resolved.size() << " resolved"
                  << " [extract=" << us(t0, t1) << "us sort=" << us(t1, t2)
                  << "us resolve=" << us(t2, t3) << "us]\n";
    }

    return resolved;
}

// ---------------------------------------------------------------------------
// Build typed table structures from resolved rows
// ---------------------------------------------------------------------------

static void build_system(System& sys, std::vector<FileRef>& files, std::vector<ResolvedRow>& rows) {
    // Batch-level extraction: get _id and site_id column arrays once per batch
    int cur_fi = -1, cur_bi = -1;
    std::shared_ptr<arrow::Array> id_arr, site_id_arr;
    const int64_t* created_at_raw = nullptr;
    const int32_t* put_offsets = nullptr;
    bool has_union = false;

    sys.sources.reserve(rows.size());
    sys.id.reserve(rows.size());
    sys.valid_from.reserve(rows.size());
    sys.valid_to.reserve(rows.size());
    sys.site_id.reserve(rows.size());
    sys.created_at.reserve(rows.size());

    for (auto& rr : rows) {
        if (rr.source_file != cur_fi || rr.source_batch != cur_bi) {
            cur_fi = rr.source_file;
            cur_bi = rr.source_batch;
            auto& batch = files[cur_fi].arrow_file.batches[cur_bi];
            auto op_col = col(batch, "op");
            if (op_col && op_col->type_id() == arrow::Type::DENSE_UNION) {
                auto union_arr = std::static_pointer_cast<arrow::DenseUnionArray>(op_col);
                auto put_struct = std::static_pointer_cast<arrow::StructArray>(union_arr->field(0));
                put_offsets = union_arr->raw_value_offsets();
                has_union = true;
                id_arr = put_struct->GetFieldByName("_id");
                site_id_arr = put_struct->GetFieldByName("site_id");
                created_at_raw = raw_i64(put_struct->GetFieldByName("created_at"));
            } else {
                has_union = false;
            }
        }

        int32_t off = has_union ? put_offsets[rr.source_row] : rr.source_row;
        int32_t row_idx = static_cast<int32_t>(sys.id.size());

        auto id = get_str(id_arr, off);
        sys.id.push_back(id);
        sys.valid_from.push_back(rr.valid_from);
        sys.valid_to.push_back(rr.valid_to);
        sys.site_id.push_back(get_str(site_id_arr, off));
        sys.created_at.push_back(created_at_raw ? created_at_raw[off] : 0);
        sys.sources.push_back({static_cast<int16_t>(rr.source_file),
                               static_cast<int16_t>(rr.source_batch),
                               rr.source_row});
        sys.id_versions[id].push_back(row_idx);
    }

    for (auto& [id, indices] : sys.id_versions) {
        std::sort(indices.begin(), indices.end(), [&](int32_t a, int32_t b) {
            return sys.valid_from[a] < sys.valid_from[b];
        });
        sys.id_latest[id] = indices.back();
    }

    sys.files = std::move(files);
}

struct BatchDataCache {
    const int32_t* put_offsets = nullptr;
    bool has_union = false;
    std::shared_ptr<arrow::StringArray> sys_id_str;
    std::shared_ptr<arrow::LargeStringArray> sys_id_lstr;
    const double* val_raw = nullptr;
};

static void resolve_and_build_readings(Readings& rdg, std::vector<FileRef>& files) {
    auto us = [](auto a, auto b) { return std::chrono::duration_cast<std::chrono::microseconds>(b - a).count(); };
    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<std::vector<BatchDataCache>> batch_data(files.size());
    std::vector<Event> events;
    int64_t total_rows = 0;

    for (int fi = 0; fi < static_cast<int>(files.size()); fi++) {
        auto& af = files[fi].arrow_file;
        batch_data[fi].resize(af.batches.size());
        for (int bi = 0; bi < static_cast<int>(af.batches.size()); bi++) {
            auto& batch = af.batches[bi];
            int64_t nr = batch->num_rows();
            total_rows += nr;

            auto& bd = batch_data[fi][bi];
            auto op_col = col(batch, "op");
            const int8_t* type_codes = nullptr;
            if (op_col && op_col->type_id() == arrow::Type::DENSE_UNION) {
                auto union_arr = std::static_pointer_cast<arrow::DenseUnionArray>(op_col);
                auto put_struct = std::static_pointer_cast<arrow::StructArray>(union_arr->field(0));
                type_codes = union_arr->raw_type_codes();
                bd.put_offsets = union_arr->raw_value_offsets();
                bd.has_union = true;
                auto sid = put_struct->GetFieldByName("system_id");
                if (sid) {
                    if (sid->type_id() == arrow::Type::STRING)
                        bd.sys_id_str = std::static_pointer_cast<arrow::StringArray>(sid);
                    else if (sid->type_id() == arrow::Type::LARGE_STRING)
                        bd.sys_id_lstr = std::static_pointer_cast<arrow::LargeStringArray>(sid);
                }
                bd.val_raw = raw_f64(put_struct->GetFieldByName("value"));
            }

            auto iid_col = col(batch, "_iid");
            auto iid_arr = iid_col ? std::static_pointer_cast<arrow::FixedSizeBinaryArray>(iid_col).get() : nullptr;
            const int64_t* sf = raw_i64(col(batch, "_system_from"));
            const int64_t* vf = raw_i64(col(batch, "_valid_from"));
            const int64_t* vt = raw_i64(col(batch, "_valid_to"));

            for (int64_t r = 0; r < nr; r++) {
                events.push_back({
                    iid_arr ? read_iid(iid_arr, r) : IID{0, 0},
                    sf ? sf[r] : 0, vf ? vf[r] : 0, vt ? vt[r] : 0,
                    type_codes ? (type_codes[r] == 0) : true,
                    fi, static_cast<int32_t>(r), bi
                });
            }
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();

    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
        if (a.iid != b.iid) return a.iid < b.iid;
        return a.system_from > b.system_from;
    });

    auto t2 = std::chrono::high_resolution_clock::now();

    struct SortEntry {
        int32_t group_id;
        Timestamp valid_from;
        Timestamp valid_to;
        double value;
    };
    std::unordered_map<std::string_view, int32_t> sys_id_map;
    std::vector<std::string_view> sys_id_list;
    std::vector<SortEntry> entries;
    entries.reserve(events.size());

    Ceiling ceiling;
    Polygon polygon;
    IID current_iid = {0, 0};
    bool skip_iid = false;
    bool first = true;

    for (auto& ev : events) {
        if (first || ev.iid != current_iid) {
            ceiling.reset();
            current_iid = ev.iid;
            skip_iid = false;
            first = false;
        }
        if (skip_iid) continue;
        if (!ev.is_put) {
            ceiling.apply_log(ev.system_from, ev.valid_from, ev.valid_to);
            skip_iid = true;
            continue;
        }
        polygon.calculate_for(ceiling, ev.valid_from, ev.valid_to);
        ceiling.apply_log(ev.system_from, ev.valid_from, ev.valid_to);

        bool has_ranges = false;
        for (auto& pr : polygon.ranges) {
            if (pr.valid_from != pr.valid_to && pr.system_to == INT64_MAX) {
                has_ranges = true;
                break;
            }
        }
        if (!has_ranges) continue;

        auto& bd = batch_data[ev.source_file][ev.source_batch];
        int32_t off = bd.has_union ? bd.put_offsets[ev.source_row] : ev.source_row;
        std::string_view sv;
        if (bd.sys_id_str) sv = bd.sys_id_str->GetView(off);
        else if (bd.sys_id_lstr) sv = bd.sys_id_lstr->GetView(off);
        auto [it, inserted] = sys_id_map.try_emplace(sv, static_cast<int32_t>(sys_id_list.size()));
        if (inserted) sys_id_list.push_back(sv);
        double val = bd.val_raw ? bd.val_raw[off] : 0.0;

        for (auto& pr : polygon.ranges) {
            if (pr.valid_from != pr.valid_to && pr.system_to == INT64_MAX) {
                entries.push_back({it->second, pr.valid_from, pr.valid_to, val});
            }
        }
    }

    auto t3 = std::chrono::high_resolution_clock::now();

    std::sort(entries.begin(), entries.end(), [](const SortEntry& a, const SortEntry& b) {
        if (a.group_id != b.group_id) return a.group_id < b.group_id;
        return a.valid_from < b.valid_from;
    });

    auto t4 = std::chrono::high_resolution_clock::now();

    rdg.valid_from.resize(entries.size());
    rdg.valid_to.resize(entries.size());
    rdg.value.resize(entries.size());
    for (int32_t i = 0; i < static_cast<int32_t>(entries.size()); i++) {
        rdg.valid_from[i] = entries[i].valid_from;
        rdg.valid_to[i] = entries[i].valid_to;
        rdg.value[i] = entries[i].value;
    }

    int32_t prev_gid = -1;
    int32_t start = 0;
    for (int32_t i = 0; i <= static_cast<int32_t>(entries.size()); i++) {
        int32_t gid = (i < static_cast<int32_t>(entries.size())) ? entries[i].group_id : -2;
        if (gid != prev_gid && prev_gid >= 0) {
            rdg.system_id_range[std::string(sys_id_list[prev_gid])] = {start, i};
            start = i;
        }
        prev_gid = gid;
    }

    auto t5 = std::chrono::high_resolution_clock::now();
    std::cerr << "    resolve+build readings: " << total_rows << " raw -> " << entries.size() << " resolved\n";
    std::cerr << "      phases: extract=" << us(t0, t1) << "us sort_events=" << us(t1, t2)
              << "us resolve+data=" << us(t2, t3) << "us sort_entries=" << us(t3, t4)
              << "us write=" << us(t4, t5) << "us\n";
}

template<typename Table, typename BuildFn>
static void build_simple_table(Table& tbl, std::vector<FileRef>& files,
                                std::vector<ResolvedRow>& rows, BuildFn build_fn) {
    CachedBatch cb;
    for (auto& rr : rows) {
        cb.ensure(files, rr.source_file, rr.source_batch);
        build_fn(tbl, cb, rr.source_row, rr.valid_from, rr.valid_to);
    }
}

// ---------------------------------------------------------------------------
// Load a single table: read trie files -> resolve -> build
// ---------------------------------------------------------------------------

static std::vector<FileRef> load_files(const json& table_json) {
    std::vector<FileRef> files;
    if (!table_json.contains("data-files") && !table_json.contains("data_files")) return files;

    auto& df = table_json.contains("data-files") ? table_json["data-files"] : table_json["data_files"];
    for (auto& entry : df) {
        std::string path = entry.value("path", "");
        if (path.empty()) continue;
        FileRef fr;
        fr.path = path;
        fr.arrow_file = read_arrow_file(path);
        files.push_back(std::move(fr));
    }
    return files;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

CatalogInfo parse_catalog(const std::string& catalog_path) {
    std::ifstream f(catalog_path);
    if (!f.is_open()) throw std::runtime_error("Cannot open catalog: " + catalog_path);
    json catalog;
    f >> catalog;

    CatalogInfo cat;
    auto& p = catalog["params"];
    auto parse_ts = [](const std::string& s) -> Timestamp {
        try { return std::stoll(s); } catch (...) { return 0; }
    };
    cat.params.sample_system_id = p.value("sample-system-id", p.value("sample_system_id", ""));
    cat.params.min_valid_time = parse_ts(p.value("min-valid-time", p.value("min_valid_time", "0")));
    cat.params.max_valid_time = parse_ts(p.value("max-valid-time", p.value("max_valid_time", "0")));
    cat.tables = catalog["tables"];
    return cat;
}

FusionData load_tables(const CatalogInfo& cat, const std::set<std::string>& needed) {
    FusionData data;
    auto& tables = cat.tables;

    auto load_and_resolve = [&](const std::string& name) -> std::pair<std::vector<FileRef>, std::vector<ResolvedRow>> {
        if (!needed.count(name) || !tables.contains(name)) return {};
        auto t0 = std::chrono::high_resolution_clock::now();
        auto files = load_files(tables[name]);
        auto t1 = std::chrono::high_resolution_clock::now();
        auto resolved = resolve_table(files, name);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto us = [](auto a, auto b) { return std::chrono::duration_cast<std::chrono::microseconds>(b - a).count(); };
        std::cerr << "    " << name << ": io=" << us(t0, t1) << "us resolve=" << us(t1, t2) << "us\n";
        return {std::move(files), std::move(resolved)};
    };

    auto time_build = [](const std::string& name, auto fn) {
        auto t0 = std::chrono::high_resolution_clock::now();
        fn();
        auto t1 = std::chrono::high_resolution_clock::now();
        std::cerr << "    build " << name << ": "
                  << std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() << "us\n";
    };

    if (needed.count("organisation")) {
        auto [files, rows] = load_and_resolve("organisation");
        build_simple_table(data.organisation, files, rows,
            [](Organisation& t, CachedBatch& cb, int64_t row, int64_t, int64_t) {
                auto id = cb.str("_id", row);
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.name.push_back(cb.str("name", row));
            });
    }
    if (needed.count("device_series")) {
        auto [files, rows] = load_and_resolve("device_series");
        build_simple_table(data.device_series, files, rows,
            [](DeviceSeries& t, CachedBatch& cb, int64_t row, int64_t, int64_t) {
                auto id = cb.str("_id", row);
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.organisation_id.push_back(cb.str("organisation_id", row));
                t.name.push_back(cb.str("name", row));
            });
    }
    if (needed.count("device_model")) {
        auto [files, rows] = load_and_resolve("device_model");
        build_simple_table(data.device_model, files, rows,
            [](DeviceModel& t, CachedBatch& cb, int64_t row, int64_t, int64_t) {
                auto id = cb.str("_id", row);
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.device_series_id.push_back(cb.str("device_series_id", row));
                t.name.push_back(cb.str("name", row));
                t.capacity_kw.push_back(cb.f64("capacity_kw", row));
            });
    }
    if (needed.count("site")) {
        auto [files, rows] = load_and_resolve("site");
        build_simple_table(data.site, files, rows,
            [](Site& t, CachedBatch& cb, int64_t row, int64_t, int64_t) {
                auto id = cb.str("_id", row);
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.address.push_back(cb.str("address", row));
                t.postcode.push_back(cb.str("postcode", row));
                t.state.push_back(cb.str("state", row));
            });
    }
    if (needed.count("system")) {
        auto [files, rows] = load_and_resolve("system");
        time_build("system", [&]() { build_system(data.system, files, rows); });
    }
    if (needed.count("device")) {
        auto [files, rows] = load_and_resolve("device");
        build_simple_table(data.device, files, rows,
            [](Device& t, CachedBatch& cb, int64_t row, int64_t vf, int64_t vt) {
                auto sys_id = cb.str("system_id", row);
                int32_t idx = static_cast<int32_t>(t.id.size());
                t.id.push_back(cb.str("_id", row));
                t.system_id.push_back(sys_id);
                t.device_model_id.push_back(cb.str("device_model_id", row));
                t.valid_from.push_back(vf);
                t.valid_to.push_back(vt);
                t.system_id_index[sys_id].push_back(idx);
            });
    }
    if (needed.count("readings")) {
        if (!tables.contains("readings")) {} else {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto files = load_files(tables["readings"]);
            auto t1 = std::chrono::high_resolution_clock::now();
            std::cerr << "    readings: io=" << std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() << "us\n";
            resolve_and_build_readings(data.readings, files);
        }
    }
    if (needed.count("test_suite")) {
        auto [files, rows] = load_and_resolve("test_suite");
        build_simple_table(data.test_suite, files, rows,
            [](TestSuite& t, CachedBatch& cb, int64_t row, int64_t, int64_t) {
                auto id = cb.str("_id", row);
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.purpose.push_back(cb.str("purpose", row));
                t.name.push_back(cb.str("name", row));
            });
    }
    if (needed.count("test_case")) {
        auto [files, rows] = load_and_resolve("test_case");
        build_simple_table(data.test_case, files, rows,
            [](TestCase& t, CachedBatch& cb, int64_t row, int64_t, int64_t) {
                auto id = cb.str("_id", row);
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.test_suite_id.push_back(cb.str("test_suite_id", row));
                t.name.push_back(cb.str("name", row));
            });
    }
    if (needed.count("test_suite_run")) {
        auto [files, rows] = load_and_resolve("test_suite_run");
        build_simple_table(data.test_suite_run, files, rows,
            [](TestSuiteRun& t, CachedBatch& cb, int64_t row, int64_t vf, int64_t vt) {
                auto id = cb.str("_id", row);
                auto sys_id = cb.str("system_id", row);
                int32_t idx = static_cast<int32_t>(t.id.size());
                t.id_index[id] = idx;
                t.id.push_back(id);
                t.system_id.push_back(sys_id);
                t.test_suite_id.push_back(cb.str("test_suite_id", row));
                t.status.push_back(cb.str("status", row));
                t.started_at.push_back(cb.i64("started_at", row));
                t.completed_at.push_back(cb.i64("completed_at", row));
                t.valid_from.push_back(vf);
                t.valid_to.push_back(vt);
                t.system_id_index[sys_id].push_back(idx);
            });
        for (auto& [sys_id, indices] : data.test_suite_run.system_id_index) {
            std::sort(indices.begin(), indices.end(), [&](int32_t a, int32_t b) {
                return data.test_suite_run.valid_from[a] > data.test_suite_run.valid_from[b];
            });
        }
    }
    if (needed.count("test_case_run")) {
        auto [files, rows] = load_and_resolve("test_case_run");
        build_simple_table(data.test_case_run, files, rows,
            [](TestCaseRun& t, CachedBatch& cb, int64_t row, int64_t vf, int64_t vt) {
                auto suite_run_id = cb.str("test_suite_run_id", row);
                int32_t idx = static_cast<int32_t>(t.id.size());
                t.id.push_back(cb.str("_id", row));
                t.test_suite_run_id.push_back(suite_run_id);
                t.test_case_id.push_back(cb.str("test_case_id", row));
                t.status.push_back(cb.str("status", row));
                t.executed_at.push_back(cb.i64("executed_at", row));
                t.valid_from.push_back(vf);
                t.valid_to.push_back(vt);
                t.suite_run_id_index[suite_run_id].push_back(idx);
            });
    }

    return data;
}

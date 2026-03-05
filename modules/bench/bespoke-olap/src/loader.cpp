#include "loader.h"
#include "bitemporal.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdexcept>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// Arrow helpers
// ---------------------------------------------------------------------------

static std::string get_str(const std::shared_ptr<arrow::Array>& a, int64_t i) {
    if (!a || a->IsNull(i)) return "";
    if (a->type_id() == arrow::Type::STRING)
        return std::static_pointer_cast<arrow::StringArray>(a)->GetString(i);
    if (a->type_id() == arrow::Type::LARGE_STRING)
        return std::static_pointer_cast<arrow::LargeStringArray>(a)->GetString(i);
    if (a->type_id() == arrow::Type::INT64)
        return std::to_string(std::static_pointer_cast<arrow::Int64Array>(a)->Value(i));
    if (a->type_id() == arrow::Type::DOUBLE)
        return std::to_string(std::static_pointer_cast<arrow::DoubleArray>(a)->Value(i));
    return "";
}

static int64_t get_i64(const std::shared_ptr<arrow::Array>& a, int64_t i) {
    if (!a || a->IsNull(i)) return 0;
    if (a->type_id() == arrow::Type::INT64)
        return std::static_pointer_cast<arrow::Int64Array>(a)->Value(i);
    if (a->type_id() == arrow::Type::INT32)
        return std::static_pointer_cast<arrow::Int32Array>(a)->Value(i);
    if (a->type_id() == arrow::Type::TIMESTAMP)
        return std::static_pointer_cast<arrow::TimestampArray>(a)->Value(i);
    return 0;
}

static double get_f64(const std::shared_ptr<arrow::Array>& a, int64_t i) {
    if (!a || a->IsNull(i)) return 0.0;
    if (a->type_id() == arrow::Type::DOUBLE)
        return std::static_pointer_cast<arrow::DoubleArray>(a)->Value(i);
    if (a->type_id() == arrow::Type::FLOAT)
        return std::static_pointer_cast<arrow::FloatArray>(a)->Value(i);
    if (a->type_id() == arrow::Type::INT64)
        return static_cast<double>(std::static_pointer_cast<arrow::Int64Array>(a)->Value(i));
    return 0.0;
}

static IID get_iid(const std::shared_ptr<arrow::Array>& a, int64_t i) {
    if (!a || a->IsNull(i)) return {0, 0};
    if (a->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
        auto fsa = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(a);
        const uint8_t* p = fsa->GetValue(i);
        uint64_t high = 0, low = 0;
        for (int b = 0; b < 8; b++) high = (high << 8) | p[b];
        for (int b = 0; b < 8; b++) low = (low << 8) | p[8 + b];
        return {high, low};
    }
    return {0, 0};
}

static std::shared_ptr<arrow::Array>
col(const std::shared_ptr<arrow::RecordBatch>& b, const std::string& name) {
    auto idx = b->schema()->GetFieldIndex(name);
    return idx < 0 ? nullptr : b->column(idx);
}

// ---------------------------------------------------------------------------
// DenseUnion "op" column helpers
//
// XTDB trie files store data columns inside the "op" DenseUnion:
//   op: dense_union<put: struct<_id, site_id, ...>, delete: null, erase: null>
//
// Top-level columns: _iid, _system_from, _valid_from, _valid_to, op
// Data columns (_id, site_id, etc.) are inside op.put (child 0).
// ---------------------------------------------------------------------------

struct OpInfo {
    std::shared_ptr<arrow::DenseUnionArray> union_arr;
    std::shared_ptr<arrow::StructArray> put_struct;

    static OpInfo from_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
        OpInfo info;
        auto op_col = col(batch, "op");
        if (!op_col || op_col->type_id() != arrow::Type::DENSE_UNION) return info;
        info.union_arr = std::static_pointer_cast<arrow::DenseUnionArray>(op_col);
        info.put_struct = std::static_pointer_cast<arrow::StructArray>(info.union_arr->field(0));
        return info;
    }

    bool is_valid() const { return union_arr != nullptr; }

    std::string op_name(int64_t row) const {
        auto type_code = union_arr->type_code(row);
        auto field = union_arr->type()->field(union_arr->child_id(row));
        return field->name();
    }

    int32_t put_offset(int64_t row) const {
        return union_arr->value_offset(row);
    }

    std::shared_ptr<arrow::Array> put_field(const std::string& name) const {
        if (!put_struct) return nullptr;
        return put_struct->GetFieldByName(name);
    }
};

// ---------------------------------------------------------------------------
// Read all record batches from an Arrow IPC file
// ---------------------------------------------------------------------------

struct ArrowFile {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

static ArrowFile read_arrow_file(const std::string& path) {
    ArrowFile af;
    auto r = arrow::io::ReadableFile::Open(path);
    if (!r.ok()) {
        std::cerr << "  skip missing: " << path << "\n";
        return af;
    }
    auto reader_r = arrow::ipc::RecordBatchFileReader::Open(r.ValueOrDie());
    if (!reader_r.ok()) {
        std::cerr << "  skip unreadable: " << path << "\n";
        return af;
    }
    auto reader = reader_r.ValueOrDie();
    for (int i = 0; i < reader->num_record_batches(); i++) {
        auto br = reader->ReadRecordBatch(i);
        if (br.ok()) af.batches.push_back(br.ValueOrDie());
    }
    return af;
}

// ---------------------------------------------------------------------------
// Extract events from trie files and resolve via polygon calculation
// ---------------------------------------------------------------------------

struct FileRef {
    std::string path;
    ArrowFile arrow_file;
};

static std::vector<ResolvedRow>
resolve_table(std::vector<FileRef>& files) {
    std::vector<Event> events;
    for (int fi = 0; fi < static_cast<int>(files.size()); fi++) {
        auto& af = files[fi].arrow_file;
        for (int bi = 0; bi < static_cast<int>(af.batches.size()); bi++) {
            auto& batch = af.batches[bi];
            auto c_iid = col(batch, "_iid");
            auto c_sf = col(batch, "_system_from");
            auto c_vf = col(batch, "_valid_from");
            auto c_vt = col(batch, "_valid_to");
            OpInfo op_info = OpInfo::from_batch(batch);

            for (int64_t r = 0; r < batch->num_rows(); r++) {
                Event ev;
                ev.iid = get_iid(c_iid, r);
                ev.system_from = get_i64(c_sf, r);
                ev.valid_from = get_i64(c_vf, r);
                ev.valid_to = get_i64(c_vt, r);
                ev.is_put = op_info.is_valid() ? (op_info.op_name(r) == "put") : true;
                ev.source_file = fi;
                ev.source_row = static_cast<int32_t>(r);
                ev.source_batch = bi;
                events.push_back(ev);
            }
        }
    }

    // Sort by (iid ASC, system_from DESC)
    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
        if (a.iid != b.iid) return a.iid < b.iid;
        return a.system_from > b.system_from;
    });

    // Walk events, apply polygon calculation per IID group
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

    return resolved;
}

// ---------------------------------------------------------------------------
// Helper: get a data column value from a resolved row.
// Data columns live inside the DenseUnion's "put" struct child.
// ---------------------------------------------------------------------------

struct PutAccessor {
    OpInfo op_info;
    int64_t batch_row;
    int32_t offset;

    PutAccessor(const std::shared_ptr<arrow::RecordBatch>& batch, int64_t row)
        : op_info(OpInfo::from_batch(batch)), batch_row(row) {
        offset = op_info.is_valid() ? op_info.put_offset(row) : static_cast<int32_t>(row);
    }

    std::string str(const std::string& name) const {
        return get_str(op_info.put_field(name), offset);
    }

    int64_t i64(const std::string& name) const {
        return get_i64(op_info.put_field(name), offset);
    }

    double f64(const std::string& name) const {
        return get_f64(op_info.put_field(name), offset);
    }
};

// ---------------------------------------------------------------------------
// Build typed table structures from resolved rows
// ---------------------------------------------------------------------------

static void build_system(System& sys, std::vector<FileRef>& files, std::vector<ResolvedRow>& rows) {
    for (auto& rr : rows) {
        auto& batch = files[rr.source_file].arrow_file.batches[rr.source_batch];
        PutAccessor pa(batch, rr.source_row);
        int32_t row_idx = static_cast<int32_t>(sys.id.size());

        auto id = pa.str("_id");
        sys.id.push_back(id);
        sys.valid_from.push_back(rr.valid_from);
        sys.valid_to.push_back(rr.valid_to);
        sys.site_id.push_back(pa.str("site_id"));
        sys.created_at.push_back(pa.i64("created_at"));
        sys.type.push_back(pa.i64("type"));
        sys.updated_time.push_back(pa.f64("updated_time"));

        sys.rtg_max_w.push_back(pa.f64("rtg_max_w"));
        sys.rtg_max_wh.push_back(pa.f64("rtg_max_wh"));
        sys.rtg_max_va.push_back(pa.f64("rtg_max_va"));
        sys.rtg_max_var.push_back(pa.f64("rtg_max_var"));
        sys.rtg_max_var_neg.push_back(pa.f64("rtg_max_var_neg"));
        sys.rtg_max_a.push_back(pa.f64("rtg_max_a"));
        sys.rtg_max_v.push_back(pa.f64("rtg_max_v"));
        sys.rtg_min_v.push_back(pa.f64("rtg_min_v"));
        sys.rtg_v_nom.push_back(pa.f64("rtg_v_nom"));
        sys.rtg_max_charge_rate_w.push_back(pa.f64("rtg_max_charge_rate_w"));
        sys.rtg_max_charge_rate_va.push_back(pa.f64("rtg_max_charge_rate_va"));
        sys.rtg_max_discharge_rate_w.push_back(pa.f64("rtg_max_discharge_rate_w"));
        sys.rtg_max_discharge_rate_va.push_back(pa.f64("rtg_max_discharge_rate_va"));
        sys.rtg_min_pf_over_excited.push_back(pa.f64("rtg_min_pf_over_excited"));
        sys.rtg_min_pf_under_excited.push_back(pa.f64("rtg_min_pf_under_excited"));

        sys.set_max_w.push_back(pa.f64("set_max_w"));
        sys.set_max_wh.push_back(pa.f64("set_max_wh"));
        sys.set_max_va.push_back(pa.f64("set_max_va"));
        sys.set_max_var.push_back(pa.f64("set_max_var"));
        sys.set_max_var_neg.push_back(pa.f64("set_max_var_neg"));
        sys.set_max_charge_rate_w.push_back(pa.f64("set_max_charge_rate_w"));
        sys.set_max_discharge_rate_w.push_back(pa.f64("set_max_discharge_rate_w"));
        sys.set_grad_w.push_back(pa.f64("set_grad_w"));

        sys.modes_enabled.push_back(pa.str("modes_enabled"));
        sys.modes_supported.push_back(pa.str("modes_supported"));
        sys.feature_a_modes_enabled.push_back(pa.str("feature_a_modes_enabled"));
        sys.feature_a_modes_supported.push_back(pa.str("feature_a_modes_supported"));
        sys.feature_b_modes_enabled.push_back(pa.str("feature_b_modes_enabled"));
        sys.feature_b_modes_supported.push_back(pa.str("feature_b_modes_supported"));

        sys.id_versions[id].push_back(row_idx);
    }

    for (auto& [id, indices] : sys.id_versions) {
        std::sort(indices.begin(), indices.end(), [&](int32_t a, int32_t b) {
            return sys.valid_from[a] < sys.valid_from[b];
        });
        sys.id_latest[id] = indices.back();
    }
}

static void build_readings(Readings& rdg, std::vector<FileRef>& files, std::vector<ResolvedRow>& rows) {
    struct Row {
        std::string id, system_id;
        Timestamp valid_from, valid_to;
        double value;
        int64_t duration;
    };
    std::vector<Row> tmp;
    tmp.reserve(rows.size());

    for (auto& rr : rows) {
        auto& batch = files[rr.source_file].arrow_file.batches[rr.source_batch];
        PutAccessor pa(batch, rr.source_row);
        tmp.push_back({
            pa.str("_id"),
            pa.str("system_id"),
            rr.valid_from, rr.valid_to,
            pa.f64("value"),
            pa.i64("duration")
        });
    }

    std::sort(tmp.begin(), tmp.end(), [](const Row& a, const Row& b) {
        if (a.system_id != b.system_id) return a.system_id < b.system_id;
        return a.valid_from < b.valid_from;
    });

    for (auto& r : tmp) {
        rdg.id.push_back(std::move(r.id));
        rdg.system_id.push_back(r.system_id);
        rdg.valid_from.push_back(r.valid_from);
        rdg.valid_to.push_back(r.valid_to);
        rdg.value.push_back(r.value);
        rdg.duration.push_back(r.duration);
    }

    std::string prev;
    int32_t start = 0;
    for (int32_t i = 0; i <= static_cast<int32_t>(rdg.system_id.size()); i++) {
        std::string cur = (i < static_cast<int32_t>(rdg.system_id.size())) ? rdg.system_id[i] : "";
        if (cur != prev && !prev.empty()) {
            rdg.system_id_range[prev] = {start, i};
            start = i;
        }
        if (cur != prev) { prev = cur; start = i; }
    }
}

// Generic builder for simple reference tables
template<typename Table, typename BuildFn>
static void build_simple_table(Table& tbl, std::vector<FileRef>& files,
                                std::vector<ResolvedRow>& rows, BuildFn build_fn) {
    for (auto& rr : rows) {
        auto& batch = files[rr.source_file].arrow_file.batches[rr.source_batch];
        PutAccessor pa(batch, rr.source_row);
        build_fn(tbl, pa, rr.valid_from, rr.valid_to);
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

FusionData load_from_catalog(const std::string& catalog_path, QueryParams& params) {
    std::ifstream f(catalog_path);
    if (!f.is_open()) throw std::runtime_error("Cannot open catalog: " + catalog_path);
    json catalog;
    f >> catalog;

    auto& p = catalog["params"];
    params.sample_system_id = p.value("sample-system-id", p.value("sample_system_id", ""));
    auto parse_ts = [](const std::string& s) -> Timestamp {
        try { return std::stoll(s); } catch (...) { return 0; }
    };
    params.min_valid_time = parse_ts(p.value("min-valid-time", p.value("min_valid_time", "0")));
    params.max_valid_time = parse_ts(p.value("max-valid-time", p.value("max_valid_time", "0")));

    auto& tables = catalog["tables"];
    FusionData data;

    auto load_and_resolve = [&](const std::string& name) -> std::pair<std::vector<FileRef>, std::vector<ResolvedRow>> {
        if (!tables.contains(name)) return {};
        auto files = load_files(tables[name]);
        std::cerr << "  " << name << ": " << files.size() << " trie files";
        auto resolved = resolve_table(files);
        std::cerr << " -> " << resolved.size() << " resolved rows\n";
        return {std::move(files), std::move(resolved)};
    };

    std::cerr << "Loading tables from trie files...\n";

    // Organisation
    {
        auto [files, rows] = load_and_resolve("organisation");
        build_simple_table(data.organisation, files, rows,
            [](Organisation& t, const PutAccessor& pa, int64_t, int64_t) {
                auto id = pa.str("_id");
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.name.push_back(pa.str("name"));
            });
    }
    // DeviceSeries
    {
        auto [files, rows] = load_and_resolve("device_series");
        build_simple_table(data.device_series, files, rows,
            [](DeviceSeries& t, const PutAccessor& pa, int64_t, int64_t) {
                auto id = pa.str("_id");
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.organisation_id.push_back(pa.str("organisation_id"));
                t.name.push_back(pa.str("name"));
            });
    }
    // DeviceModel
    {
        auto [files, rows] = load_and_resolve("device_model");
        build_simple_table(data.device_model, files, rows,
            [](DeviceModel& t, const PutAccessor& pa, int64_t, int64_t) {
                auto id = pa.str("_id");
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.device_series_id.push_back(pa.str("device_series_id"));
                t.name.push_back(pa.str("name"));
                t.capacity_kw.push_back(pa.f64("capacity_kw"));
            });
    }
    // Site
    {
        auto [files, rows] = load_and_resolve("site");
        build_simple_table(data.site, files, rows,
            [](Site& t, const PutAccessor& pa, int64_t, int64_t) {
                auto id = pa.str("_id");
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.address.push_back(pa.str("address"));
                t.postcode.push_back(pa.str("postcode"));
                t.state.push_back(pa.str("state"));
            });
    }
    // System
    {
        auto [files, rows] = load_and_resolve("system");
        build_system(data.system, files, rows);
    }
    // Device
    {
        auto [files, rows] = load_and_resolve("device");
        build_simple_table(data.device, files, rows,
            [](Device& t, const PutAccessor& pa, int64_t vf, int64_t vt) {
                auto sys_id = pa.str("system_id");
                int32_t idx = static_cast<int32_t>(t.id.size());
                t.id.push_back(pa.str("_id"));
                t.system_id.push_back(sys_id);
                t.device_model_id.push_back(pa.str("device_model_id"));
                t.valid_from.push_back(vf);
                t.valid_to.push_back(vt);
                t.system_id_index[sys_id].push_back(idx);
            });
    }
    // Readings
    {
        auto [files, rows] = load_and_resolve("readings");
        build_readings(data.readings, files, rows);
    }
    // TestSuite
    {
        auto [files, rows] = load_and_resolve("test_suite");
        build_simple_table(data.test_suite, files, rows,
            [](TestSuite& t, const PutAccessor& pa, int64_t, int64_t) {
                auto id = pa.str("_id");
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.purpose.push_back(pa.str("purpose"));
                t.name.push_back(pa.str("name"));
            });
    }
    // TestCase
    {
        auto [files, rows] = load_and_resolve("test_case");
        build_simple_table(data.test_case, files, rows,
            [](TestCase& t, const PutAccessor& pa, int64_t, int64_t) {
                auto id = pa.str("_id");
                t.id_index[id] = static_cast<int32_t>(t.id.size());
                t.id.push_back(id);
                t.test_suite_id.push_back(pa.str("test_suite_id"));
                t.name.push_back(pa.str("name"));
            });
    }
    // TestSuiteRun
    {
        auto [files, rows] = load_and_resolve("test_suite_run");
        build_simple_table(data.test_suite_run, files, rows,
            [](TestSuiteRun& t, const PutAccessor& pa, int64_t vf, int64_t vt) {
                auto id = pa.str("_id");
                auto sys_id = pa.str("system_id");
                int32_t idx = static_cast<int32_t>(t.id.size());
                t.id_index[id] = idx;
                t.id.push_back(id);
                t.system_id.push_back(sys_id);
                t.test_suite_id.push_back(pa.str("test_suite_id"));
                t.status.push_back(pa.str("status"));
                t.started_at.push_back(pa.i64("started_at"));
                t.completed_at.push_back(pa.i64("completed_at"));
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
    // TestCaseRun
    {
        auto [files, rows] = load_and_resolve("test_case_run");
        build_simple_table(data.test_case_run, files, rows,
            [](TestCaseRun& t, const PutAccessor& pa, int64_t vf, int64_t vt) {
                auto suite_run_id = pa.str("test_suite_run_id");
                int32_t idx = static_cast<int32_t>(t.id.size());
                t.id.push_back(pa.str("_id"));
                t.test_suite_run_id.push_back(suite_run_id);
                t.test_case_id.push_back(pa.str("test_case_id"));
                t.status.push_back(pa.str("status"));
                t.executed_at.push_back(pa.i64("executed_at"));
                t.valid_from.push_back(vf);
                t.valid_to.push_back(vt);
                t.suite_run_id_index[suite_run_id].push_back(idx);
            });
    }

    return data;
}

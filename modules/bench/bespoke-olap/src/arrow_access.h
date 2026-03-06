#pragma once

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

inline std::string_view get_str_view(const std::shared_ptr<arrow::Array>& a, int64_t i) {
    if (!a || a->IsNull(i)) return {};
    if (a->type_id() == arrow::Type::STRING)
        return std::static_pointer_cast<arrow::StringArray>(a)->GetView(i);
    if (a->type_id() == arrow::Type::LARGE_STRING)
        return std::static_pointer_cast<arrow::LargeStringArray>(a)->GetView(i);
    return {};
}

inline std::string get_str(const std::shared_ptr<arrow::Array>& a, int64_t i) {
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

inline int64_t get_i64(const std::shared_ptr<arrow::Array>& a, int64_t i) {
    if (!a || a->IsNull(i)) return 0;
    if (a->type_id() == arrow::Type::INT64)
        return std::static_pointer_cast<arrow::Int64Array>(a)->Value(i);
    if (a->type_id() == arrow::Type::INT32)
        return std::static_pointer_cast<arrow::Int32Array>(a)->Value(i);
    if (a->type_id() == arrow::Type::TIMESTAMP)
        return std::static_pointer_cast<arrow::TimestampArray>(a)->Value(i);
    return 0;
}

inline double get_f64(const std::shared_ptr<arrow::Array>& a, int64_t i) {
    if (!a || a->IsNull(i)) return 0.0;
    if (a->type_id() == arrow::Type::DOUBLE)
        return std::static_pointer_cast<arrow::DoubleArray>(a)->Value(i);
    if (a->type_id() == arrow::Type::FLOAT)
        return std::static_pointer_cast<arrow::FloatArray>(a)->Value(i);
    if (a->type_id() == arrow::Type::INT64)
        return static_cast<double>(std::static_pointer_cast<arrow::Int64Array>(a)->Value(i));
    return 0.0;
}

inline std::shared_ptr<arrow::Array>
col(const std::shared_ptr<arrow::RecordBatch>& b, const std::string& name) {
    auto idx = b->schema()->GetFieldIndex(name);
    return idx < 0 ? nullptr : b->column(idx);
}

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
    bool is_put_op(int64_t row) const { return !union_arr || union_arr->child_id(row) == 0; }
    int32_t put_offset(int64_t row) const { return union_arr->value_offset(row); }

    std::shared_ptr<arrow::Array> put_field(const std::string& name) const {
        if (!put_struct) return nullptr;
        return put_struct->GetFieldByName(name);
    }
};

struct ArrowFile {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

struct FileRef {
    std::string path;
    ArrowFile arrow_file;
};

struct RowSource {
    int16_t file;
    int16_t batch;
    int32_t row;
};

struct CachedBatch {
    OpInfo op;
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> field_cache;
    int cur_file = -1;
    int cur_batch = -1;

    void ensure(const std::vector<FileRef>& files, int file, int batch) {
        if (file == cur_file && batch == cur_batch) return;
        cur_file = file;
        cur_batch = batch;
        field_cache.clear();
        op = OpInfo::from_batch(files[file].arrow_file.batches[batch]);
    }

    int32_t offset(int64_t row) const {
        return op.is_valid() ? op.put_offset(row) : static_cast<int32_t>(row);
    }

    const std::shared_ptr<arrow::Array>& field(const std::string& name) {
        auto it = field_cache.find(name);
        if (it != field_cache.end()) return it->second;
        return field_cache.emplace(name, op.put_field(name)).first->second;
    }

    std::string_view str_view(const std::string& name, int64_t row) {
        return get_str_view(field(name), offset(row));
    }
    std::string str(const std::string& name, int64_t row) {
        return get_str(field(name), offset(row));
    }
    int64_t i64(const std::string& name, int64_t row) {
        return get_i64(field(name), offset(row));
    }
    double f64(const std::string& name, int64_t row) {
        return get_f64(field(name), offset(row));
    }
};

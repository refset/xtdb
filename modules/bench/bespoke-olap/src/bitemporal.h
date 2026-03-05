#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

// Port of XTDB's Ceiling + Polygon bitemporal resolution from Kotlin.
// Events are processed in system-time-descending order within each IID group.

struct Ceiling {
    // Stored in descending valid-time order (like the Kotlin version)
    std::vector<int64_t> valid_times;
    std::vector<int64_t> sys_time_ceilings;

    Ceiling() { reset(); }

    void reset() {
        valid_times.clear();
        valid_times.push_back(INT64_MAX);
        valid_times.push_back(INT64_MIN);
        sys_time_ceilings.clear();
        sys_time_ceilings.push_back(INT64_MAX);
    }

    int reverse_idx(int idx) const {
        return static_cast<int>(valid_times.size()) - 1 - idx;
    }

    int64_t get_valid_from(int range_idx) const {
        return valid_times[reverse_idx(range_idx)];
    }

    int64_t get_valid_to(int range_idx) const {
        return valid_times[reverse_idx(range_idx + 1)];
    }

    int64_t get_system_time(int range_idx) const {
        return sys_time_ceilings[reverse_idx(range_idx) - 1];
    }

    // Binary search in descending-sorted valid_times
    // Returns index if found, otherwise -(insertion_point) - 1
    int binary_search(int64_t needle) const {
        int left = 0;
        int right = static_cast<int>(valid_times.size());
        while (left < right) {
            int mid = (left + right) / 2;
            int64_t x = valid_times[mid];
            if (x == needle) return mid;
            if (x > needle) left = mid + 1;
            else right = mid;
        }
        return -left - 1;
    }

    int get_ceiling_index(int64_t valid_time) const {
        int idx = binary_search(valid_time);
        if (idx < 0) idx = -(idx + 1);
        if (idx < static_cast<int>(valid_times.size()) - 1 && valid_time < valid_times[idx]) idx++;
        if (idx == static_cast<int>(valid_times.size())) idx--;
        return reverse_idx(idx);
    }

    void apply_log(int64_t system_from, int64_t valid_from, int64_t valid_to) {
        if (valid_from >= valid_to) return;

        int end = binary_search(valid_to);
        bool inserted_end = end < 0;
        if (inserted_end) end = -(end + 1);

        int start = binary_search(valid_from);
        bool inserted_start = start < 0;
        if (inserted_start) start = -(start + 1);

        if (!inserted_end && !inserted_start) {
            sys_time_ceilings[end] = system_from;
        } else if (!inserted_end) {
            valid_times.insert(valid_times.begin() + start, valid_from);
            sys_time_ceilings.insert(sys_time_ceilings.begin() + end, system_from);
        } else if (!inserted_start) {
            valid_times.insert(valid_times.begin() + end, valid_to);
            sys_time_ceilings.insert(sys_time_ceilings.begin() + end, system_from);
            start++;
        } else if (end == start) {
            valid_times.insert(valid_times.begin() + end, valid_to);
            sys_time_ceilings.insert(sys_time_ceilings.begin() + end, system_from);
            start++;
            valid_times.insert(valid_times.begin() + start, valid_from);
            sys_time_ceilings.insert(sys_time_ceilings.begin() + start, sys_time_ceilings[end - 1]);
        } else {
            valid_times.insert(valid_times.begin() + end, valid_to);
            sys_time_ceilings.insert(sys_time_ceilings.begin() + end, system_from);
            valid_times[start] = valid_from;
        }

        // Remove range (end+1, start) — elements between end and start exclusive
        if (end + 1 < start) {
            valid_times.erase(valid_times.begin() + end + 1, valid_times.begin() + start);
            sys_time_ceilings.erase(sys_time_ceilings.begin() + end + 1, sys_time_ceilings.begin() + start);
        }
    }
};

struct PolygonRange {
    int64_t valid_from;
    int64_t valid_to;
    int64_t system_to;
};

struct Polygon {
    std::vector<PolygonRange> ranges;

    void calculate_for(const Ceiling& ceiling, int64_t valid_from, int64_t valid_to) {
        ranges.clear();
        int64_t vt = valid_from;
        int ceil_idx = ceiling.get_ceiling_index(valid_from);

        while (vt < valid_to) {
            int64_t ceil_valid_to;
            while (true) {
                ceil_valid_to = ceiling.get_valid_to(ceil_idx);
                if (ceil_valid_to > vt) break;
                ceil_idx++;
            }
            int64_t sys_to = ceiling.get_system_time(ceil_idx);
            int64_t next_vt = std::min(ceil_valid_to, valid_to);
            ranges.push_back({vt, next_vt, sys_to});
            vt = next_vt;
        }
    }
};

// IID is a 128-bit value (two int64s in unsigned comparison order)
struct IID {
    uint64_t high;
    uint64_t low;

    bool operator==(const IID& o) const { return high == o.high && low == o.low; }
    bool operator!=(const IID& o) const { return !(*this == o); }
    bool operator<(const IID& o) const {
        if (high != o.high) return high < o.high;
        return low < o.low;
    }
};

// A raw event from a trie data file
struct Event {
    IID iid;
    int64_t system_from;
    int64_t valid_from;
    int64_t valid_to;
    bool is_put; // true = put, false = erase
    int32_t source_file;  // index into data files list
    int32_t source_row;   // row index within the record batch
    int32_t source_batch; // batch index within the file
};

// A resolved row after polygon calculation
struct ResolvedRow {
    int64_t valid_from;
    int64_t valid_to;
    int32_t source_file;
    int32_t source_row;
    int32_t source_batch;
};

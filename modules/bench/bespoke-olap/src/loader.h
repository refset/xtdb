#pragma once

#include "types.h"

#include <nlohmann/json.hpp>
#include <set>
#include <string>

struct CatalogInfo {
    nlohmann::json tables;
    QueryParams params;
};

CatalogInfo parse_catalog(const std::string& catalog_path);

// Load only the specified tables from trie files.
// Tables not in `needed` are left empty in the returned FusionData.
FusionData load_tables(const CatalogInfo& cat, const std::set<std::string>& needed);

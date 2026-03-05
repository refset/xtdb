#pragma once

#include "types.h"
#include <string>

// Load catalog JSON (written by the Clojure catalog generator),
// read Arrow IPC trie files directly from the XTDB object store,
// resolve bitemporality via polygon calculation,
// and build in-memory query structures.
FusionData load_from_catalog(const std::string& catalog_path, QueryParams& params);

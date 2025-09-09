#!/bin/bash

# Start XTDB Node via CLI with Configuration File
# Usage: ./start-xtdb-node.sh [config-file]

set -euo pipefail

# Default config file if none provided
CONFIG_FILE="${1:-xtdb-file-cdc-config.yaml}"

echo "🚀 Starting XTDB Node"
echo "==================="
echo "Configuration: $CONFIG_FILE"
echo "JDBC URL: jdbc:xtdb://localhost:5432/xtdb"
echo "HTTP API: http://localhost:3000"
echo

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Configuration file '$CONFIG_FILE' not found!"
    echo "Available configuration files:"
    ls -1 *.yaml *.yml *.edn 2>/dev/null || echo "  (none found)"
    exit 1
fi

echo "📋 Configuration Preview:"
echo "------------------------"
head -20 "$CONFIG_FILE" | sed 's/^/  /'
echo "  ..."
echo

echo "🔧 Building XTDB (if needed)..."
./gradlew :xtdb-core:compileKotlin --quiet

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "✅ Build completed"
echo

echo "📝 Note: This starts a basic XTDB node. File-based CDC is not yet"
echo "   integrated into the core XTDB configuration system."
echo "   For CDC file generation, see the separate demo scripts."
echo

echo "🎯 Starting XTDB node with configuration: $CONFIG_FILE"
echo "   Press Ctrl+C to stop"
echo "=" * 60

# Start XTDB using gradle run with the same JVM options as Docker
echo "⚡ Starting XTDB with config file: $CONFIG_FILE"
echo "   Using gradle run with Docker-equivalent JVM options"
echo

./gradlew run --args="node --file $CONFIG_FILE"
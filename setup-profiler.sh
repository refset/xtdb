#!/bin/bash

# Script to set up async-profiler for XTDB startup profiling

set -e

echo "Setting up async-profiler for XTDB..."

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
  x86_64)
    PROFILER_LIB="libasyncProfiler-linux-x64.so"
    ;;
  aarch64|arm64)
    PROFILER_LIB="libasyncProfiler-linux-aarch64.so"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# Extract the native library from the jar
ASYNC_PROFILER_JAR="$HOME/.m2/repository/com/clojure-goes-fast/clj-async-profiler/1.3.0/clj-async-profiler-1.3.0.jar"
PROFILER_DIR="/tmp/async-profiler"
PROFILER_PATH="$PROFILER_DIR/$PROFILER_LIB"

if [ ! -f "$ASYNC_PROFILER_JAR" ]; then
  echo "async-profiler jar not found at: $ASYNC_PROFILER_JAR"
  echo "Make sure clj-async-profiler dependency is downloaded."
  exit 1
fi

# Create temp directory and extract library
mkdir -p "$PROFILER_DIR"
cd "$PROFILER_DIR"
jar -xf "$ASYNC_PROFILER_JAR" "$PROFILER_LIB" || {
  echo "Failed to extract $PROFILER_LIB from jar"
  exit 1
}

# Check if extraction was successful
if [ ! -f "$PROFILER_LIB" ]; then
  echo "Extraction failed - $PROFILER_LIB not found"
  echo "Available files in jar:"
  jar -tf "$ASYNC_PROFILER_JAR" | grep -E "\\.so$"
  exit 1
fi

# Move to the correct location (only if not already there)
if [ "$PROFILER_DIR/$PROFILER_LIB" != "$PROFILER_PATH" ]; then
  mv "$PROFILER_LIB" "$PROFILER_PATH"
fi

echo "✅ Async-profiler library extracted to: $PROFILER_PATH"
echo ""
echo "Now you can run XTDB with profiling using:"
echo ""
echo "# For REPL with profiling:"
echo "./gradlew clojureRepl -PasyncProfiler=$PROFILER_PATH"
echo ""
echo "# For running XTDB with profiling:"
echo "./gradlew run -PasyncProfiler=$PROFILER_PATH"
echo ""
echo "# For running playground mode with profiling:"
echo "./gradlew run -PasyncProfiler=$PROFILER_PATH --args='playground'"
echo ""
echo "The profile will be saved to: /tmp/xtdb-startup-profile.jfr"
echo "After startup, use the clj-async-profiler functions to generate flame graphs."
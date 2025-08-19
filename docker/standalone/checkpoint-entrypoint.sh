#!/bin/bash
set -e

CHECKPOINT_DIR="/var/lib/xtdb/checkpoint"
RESTORE_LOG="/tmp/criu-restore.log"
DUMP_LOG="/tmp/criu-dump.log"

# Function to start XTDB normally
start_xtdb() {
    echo "Starting XTDB..."
    exec java \
        -Dclojure.main.report=stderr \
        --add-opens=java.base/java.nio=ALL-UNNAMED \
        -Dio.netty.tryReflectionSetAccessible=true \
        -cp xtdb-standalone.jar \
        clojure.main -m xtdb.main \
        "$@"
}

# Function to wait for XTDB to be ready
wait_for_xtdb_ready() {
    echo "Waiting for XTDB to be ready..."
    max_attempts=90
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -f -s http://localhost:8080/healthz/alive > /dev/null 2>&1; then
            echo "XTDB health check passed"
            return 0
        fi
        echo "Attempt $((attempt + 1))/$max_attempts - waiting for health check..."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo "ERROR: XTDB failed to become healthy within timeout"
    return 1
}

# Function to create CRIU checkpoint
create_criu_checkpoint() {
    local pid=$1
    echo "Creating CRIU checkpoint for PID $pid..."
    
    mkdir -p "$CHECKPOINT_DIR"
    
    # Create checkpoint with CRIU
    if criu dump \
        --tree $pid \
        --images-dir "$CHECKPOINT_DIR" \
        --shell-job \
        --file-locks \
        --tcp-established \
        --ext-unix-sk \
        --log-file "$DUMP_LOG" \
        -vvv; then
        echo "✅ CRIU checkpoint created successfully"
        return 0
    else
        echo "❌ CRIU checkpoint failed, see $DUMP_LOG for details"
        [ -f "$DUMP_LOG" ] && tail -20 "$DUMP_LOG"
        return 1
    fi
}

# Function to restore from CRIU checkpoint
restore_from_criu_checkpoint() {
    echo "🚀 Restoring XTDB from CRIU checkpoint..."
    
    if [ ! -d "$CHECKPOINT_DIR" ] || [ ! -f "$CHECKPOINT_DIR/core-1.img" ]; then
        echo "No CRIU checkpoint found, falling back to normal startup"
        return 1
    fi
    
    cd "$CHECKPOINT_DIR"
    
    # Restore process from checkpoint
    if criu restore \
        --images-dir "$CHECKPOINT_DIR" \
        --shell-job \
        --file-locks \
        --tcp-established \
        --ext-unix-sk \
        --log-file "$RESTORE_LOG" \
        -vvv; then
        echo "✅ XTDB restored from CRIU checkpoint successfully!"
        return 0
    else
        echo "❌ CRIU restore failed, see $RESTORE_LOG for details"
        [ -f "$RESTORE_LOG" ] && tail -20 "$RESTORE_LOG"
        echo "Falling back to normal startup..."
        return 1
    fi
}

# Main logic
case "$1" in
    "--create-checkpoint")
        echo "=== CRIU Checkpoint Creation Mode ==="
        
        # Start XTDB in the background
        java \
            -Dclojure.main.report=stderr \
            --add-opens=java.base/java.nio=ALL-UNNAMED \
            -Dio.netty.tryReflectionSetAccessible=true \
            -cp xtdb-standalone.jar \
            clojure.main -m xtdb.main \
            -f local_config.yaml &
        
        XTDB_PID=$!
        echo "XTDB started with PID $XTDB_PID"
        
        # Wait for XTDB to be healthy and warm up
        if wait_for_xtdb_ready; then
            echo "XTDB is healthy, waiting additional 30 seconds for warmup..."
            sleep 30
            echo "Warmup complete - creating CRIU checkpoint..."
            
            if create_criu_checkpoint $XTDB_PID; then
                echo "✅ Checkpoint creation successful"
                # Kill the process since we have the checkpoint
                kill $XTDB_PID 2>/dev/null || true
                wait $XTDB_PID 2>/dev/null || true
                echo "Original process terminated"
                exit 0
            else
                echo "❌ Checkpoint creation failed"
                kill $XTDB_PID 2>/dev/null || true
                exit 1
            fi
        else
            echo "❌ XTDB failed to start properly"
            kill $XTDB_PID 2>/dev/null || true
            exit 1
        fi
        ;;
        
    "--restore-only")
        echo "=== CRIU Restore Only Mode ==="
        if restore_from_criu_checkpoint; then
            # If restore succeeds, the process should be running
            echo "XTDB restored and running from checkpoint"
            # Wait forever since the restored process is now running
            wait
        else
            echo "Restore failed"
            exit 1
        fi
        ;;
        
    *)
        echo "=== Normal XTDB Startup (with checkpoint attempt) ==="
        
        # Try to restore from checkpoint first
        if restore_from_criu_checkpoint; then
            echo "✅ Fast startup from CRIU checkpoint!"
            # The restored process is now running, wait for it
            wait
        else
            echo "🐌 No checkpoint available, using normal startup..."
            start_xtdb "$@"
        fi
        ;;
esac
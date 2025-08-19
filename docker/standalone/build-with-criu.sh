#!/bin/bash
set -e

echo "Building XTDB standalone Docker image with CRIU checkpoint..."

# Check if we have the necessary privileges for CRIU
echo "Checking CRIU requirements..."

# Build the base image first using CRIU-enabled Dockerfile
echo "Building base XTDB image with CRIU..."
docker build --load -f Dockerfile.criu -t xtdb-standalone-base .

echo "Creating CRIU checkpoint container..."

# CRIU requires specific capabilities and privileges
# We'll create a checkpoint by running in privileged mode
CONTAINER_ID=$(docker run -d \
    --name xtdb-criu-warmup-$$ \
    --privileged \
    --cap-add=ALL \
    --security-opt seccomp=unconfined \
    --security-opt apparmor=unconfined \
    --tmpfs /tmp \
    -p 8080:8080 \
    xtdb-standalone-base \
    --create-checkpoint)

echo "Container started: $CONTAINER_ID"
echo "This will take about 60-90 seconds (startup + warmup + checkpoint creation)..."

# Wait for the checkpoint creation to complete
echo "Waiting for CRIU checkpoint creation..."
EXIT_CODE=$(docker wait $CONTAINER_ID)

if [ "$EXIT_CODE" = "0" ]; then
    echo "✅ CRIU checkpoint creation completed successfully"
    
    # Commit the container with the checkpoint data
    echo "Creating final image with CRIU checkpoint..."
    docker commit \
        --change='CMD ["-f", "local_config.yaml"]' \
        --change='LABEL checkpoint_type=criu' \
        $CONTAINER_ID \
        xtdb-standalone:criu-checkpoint
    
    # Clean up
    docker rm $CONTAINER_ID
    
    echo "✅ XTDB standalone image with CRIU checkpoint created successfully!"
    echo ""
    echo "🚀 USAGE:"
    echo "  Fast startup (2-5s): docker run --privileged --cap-add=ALL -p 5432:5432 -p 8080:8080 -p 3000:3000 xtdb-standalone:criu-checkpoint"
    echo "  Normal startup: docker run -p 5432:5432 -p 8080:8080 -p 3000:3000 xtdb-standalone-base"
    echo ""
    echo "⚠️  IMPORTANT: CRIU restore requires --privileged and --cap-add=ALL flags"
    echo ""
    
else
    echo "❌ CRIU checkpoint creation failed with exit code $EXIT_CODE"
    echo "Container logs:"
    docker logs $CONTAINER_ID
    docker rm $CONTAINER_ID
    echo ""
    echo "This might be due to:"
    echo "1. CRIU not being available in the kernel"
    echo "2. Insufficient privileges"
    echo "3. Security restrictions"
    echo "4. Incompatible environment"
    echo ""
    echo "Falling back to regular pre-warmed image..."
    
    # Fallback to the previous approach
    echo "Creating fallback pre-warmed image..."
    docker tag xtdb-standalone-base xtdb-standalone:criu-checkpoint
    echo "Created fallback image (no CRIU checkpoint)"
fi

echo "Cleaning up temporary resources..."
docker rmi xtdb-standalone-base 2>/dev/null || true

echo ""
echo "Build complete!"
echo ""
echo "🔍 To test the performance difference:"
echo "  time docker run --rm --privileged --cap-add=ALL -p 8080:8080 xtdb-standalone:criu-checkpoint --restore-only"
echo ""
echo "📚 See CRIU-README.md for detailed usage instructions"
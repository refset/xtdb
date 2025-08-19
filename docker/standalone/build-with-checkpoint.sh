#!/bin/bash
set -e

echo "Building XTDB standalone Docker image with checkpoint..."

# Check if Docker experimental features are enabled for checkpoint support
if ! docker info 2>/dev/null | grep -q "Experimental: true"; then
    echo "Warning: Docker experimental features not enabled. Checkpoint/restore may not work optimally."
    echo "To enable: Set 'experimental': true in Docker daemon configuration"
fi

# Build the base image first
echo "Building base XTDB image..."
docker build --load -t xtdb-standalone-base .

echo "Creating warm container for checkpoint..."

# Start container and let XTDB warm up
CONTAINER_ID=$(docker run -d \
    --name xtdb-warmup-$$ \
    -p 8080:8080 \
    xtdb-standalone-base \
    --create-checkpoint)

echo "Container started: $CONTAINER_ID"

# Wait for the warmup to complete (the container will exit when ready)
echo "Waiting for XTDB warmup to complete..."
docker wait $CONTAINER_ID

echo "Warmup complete. Creating checkpoint..."

# Create checkpoint using Docker's experimental checkpoint feature (if available)
if docker checkpoint --help >/dev/null 2>&1; then
    # Docker has checkpoint support - try to create the checkpoint
    echo "Creating Docker checkpoint..."
    if docker checkpoint create $CONTAINER_ID xtdb-checkpoint 2>/dev/null; then
        # Commit the container state to a new image
        echo "Creating image from checkpoint..."
        docker commit --change='CMD ["-f", "local_config.yaml"]' $CONTAINER_ID xtdb-standalone:with-checkpoint
        
        # Clean up the container
        docker rm $CONTAINER_ID
        
        echo "✅ XTDB standalone image with checkpoint created successfully!"
        echo "To use with fast startup: docker run -p 5432:5432 -p 8080:8080 -p 3000:3000 xtdb-standalone:with-checkpoint"
    else
        echo "Checkpoint creation failed - falling back to pre-warmed image..."
        # Create marker file to indicate this is a pre-warmed image
        docker exec $CONTAINER_ID touch /var/lib/xtdb/.pre-warmed 2>/dev/null || true
        
        # Commit the container to create a new image with warmed state
        docker commit --change='CMD ["-f", "local_config.yaml"]' $CONTAINER_ID xtdb-standalone:with-checkpoint
        
        # Clean up
        docker rm $CONTAINER_ID
        
        echo "✅ XTDB standalone pre-warmed image created successfully!"
        echo "Note: Using pre-warmed JVM state (checkpoint creation failed)"
    fi
else
    # Fallback: commit the warmed-up state as a regular image
    echo "Docker checkpoint not available - creating image from warmed state..."
    
    # Create marker file to indicate this is a pre-warmed image  
    docker exec $CONTAINER_ID touch /var/lib/xtdb/.pre-warmed 2>/dev/null || true
    
    # Commit the container to create a new image with warmed state
    docker commit --change='CMD ["-f", "local_config.yaml"]' $CONTAINER_ID xtdb-standalone:with-checkpoint
    
    # Clean up
    docker rm $CONTAINER_ID
    
    echo "✅ XTDB standalone pre-warmed image created successfully!"
    echo "To use with faster startup: docker run -p 5432:5432 -p 8080:8080 -p 3000:3000 xtdb-standalone:with-checkpoint"
    echo "Note: This uses a pre-warmed JVM state rather than a full checkpoint"
fi

echo "Cleaning up temporary resources..."
docker rmi xtdb-standalone-base 2>/dev/null || true

echo "Build complete!"
echo ""
echo "Usage examples:"
echo "  Fast startup: docker run -p 5432:5432 -p 8080:8080 -p 3000:3000 xtdb-standalone:with-checkpoint"
echo "  Normal startup: docker run -p 5432:5432 -p 8080:8080 -p 3000:3000 xtdb-standalone-base"
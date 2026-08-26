#!/bin/bash

# Load environment variables from .env file
set -a
. ./.env
set +a

# Check if required environment variables are set
if [ -z "$HBASE_IMAGE" ]; then
    echo "Error: HBASE_IMAGE is not set in .env file."
    exit 1
fi

# HBase source directory
HBASE_SOURCE_DIR="./hbase"

# Check if HBase source directory exists
if [ ! -d "$HBASE_SOURCE_DIR" ]; then
    echo "Error: HBase source directory does not exist at $HBASE_SOURCE_DIR."
    exit 1
fi

# Run Maven clean to remove previous build artifacts
echo "Running 'mvn clean' in $HBASE_SOURCE_DIR..."
cd "$HBASE_SOURCE_DIR" || exit 1
mvn clean

if [ $? -ne 0 ]; then
    echo "Error: 'mvn clean' failed."
    exit 1
else
    echo "'mvn clean' completed successfully."
fi

# Return to the original directory
cd - || exit 1

# Build HBase Docker image
echo "Building HBase Docker image: ${HBASE_IMAGE}"
docker build -t "${HBASE_IMAGE}" ./

if [ $? -ne 0 ]; then
    echo "Error: Failed to build HBase Docker image."
    exit 1
else
    echo "HBase Docker image built successfully."
fi

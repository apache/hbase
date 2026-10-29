#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Load environment variables from .env file
set -a
. ./.env
set +a

# Check if required environment variables are set
if [ -z "$HBASE_IMAGE" ]; then
    echo "Error: HBASE_IMAGE is not set in .env file."
    exit 1
fi

# Trimmed HBase tree under read-replica/hbase (see run_read_replica_integration_tests.sh).
HBASE_SOURCE_DIR="${HBASE_SOURCE_DIR:-./hbase}"

if [ ! -f "${HBASE_SOURCE_DIR}/pom.xml" ]; then
    echo "Error: HBase source (pom.xml) not found at ${HBASE_SOURCE_DIR}."
    exit 1
fi

# Run Maven clean to remove previous build artifacts
echo "Running 'mvn clean' in $HBASE_SOURCE_DIR..."
MVN_CLEAN_START=${SECONDS}
cd "$HBASE_SOURCE_DIR" || exit 1
mvn clean
MVN_CLEAN_EXIT=$?
MVN_CLEAN_SEC=$((SECONDS - MVN_CLEAN_START))

if [ ${MVN_CLEAN_EXIT} -ne 0 ]; then
    echo "Error: 'mvn clean' failed."
    exit 1
else
    echo "'mvn clean' completed successfully (${MVN_CLEAN_SEC}s)."
fi

# Return to the original directory
cd - || exit 1

# Build HBase Docker image (Maven package runs inside the Dockerfile)
echo "Building HBase Docker image: ${HBASE_IMAGE}"
DOCKER_BUILD_START=${SECONDS}
DOCKER_BUILDKIT=1 docker build -t "${HBASE_IMAGE}" ./
DOCKER_BUILD_EXIT=$?
DOCKER_BUILD_SEC=$((SECONDS - DOCKER_BUILD_START))

if [ -n "${READ_REPLICA_TIMING_FILE}" ]; then
    mkdir -p "$(dirname "${READ_REPLICA_TIMING_FILE}")"
    cat > "${READ_REPLICA_TIMING_FILE}" <<EOF
MVN_CLEAN_SEC=${MVN_CLEAN_SEC}
DOCKER_BUILD_SEC=${DOCKER_BUILD_SEC}
BUILD_IMAGES_SEC=$((MVN_CLEAN_SEC + DOCKER_BUILD_SEC))
EOF
fi

if [ ${DOCKER_BUILD_EXIT} -ne 0 ]; then
    echo "Error: Failed to build HBase Docker image."
    exit 1
else
    echo "HBase Docker image built successfully (${DOCKER_BUILD_SEC}s)."
fi

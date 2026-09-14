#!/usr/bin/env bash
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
#
# Run the read-replica Docker integration test suite.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPLICA_DIR="${SCRIPT_DIR}/read-replica"
OUTPUT_DIR="${OUTPUT_DIR:-${REPLICA_DIR}/output}"
export HBASE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export HBASE_IMAGE="hbase-read-replica:${BUILD_NUMBER:-local}"

echo "Script dir: ${SCRIPT_DIR}"
echo "Replica dir: ${REPLICA_DIR}"
echo "Output dir: ${OUTPUT_DIR}"
echo "HBase root: ${HBASE_ROOT}"

echo "Changing to replica dir: REPLICA_DIR"
cd "${REPLICA_DIR}"

echo "Sourcing environment file: $(pwd)/.env"
set -a
source .env
set +a

echo "HBASE_IMAGE=${HBASE_IMAGE}"
echo "ACTIVE_CLUSTER_CONF_DIR=${ACTIVE_CLUSTER_CONF_DIR}"
echo "REPLICA_CLUSTER_CONF_DIR=${REPLICA_CLUSTER_CONF_DIR}"
echo "DOCKER_COMPOSE_FILE=${DOCKER_COMPOSE_FILE}"
echo "HBASE_DATA_STORE_ROOT=${HBASE_DATA_STORE_ROOT}"
echo "realpath of HBASE_DATA_STORE_ROOT=$(realpath ${HBASE_DATA_STORE_ROOT})"

echo "Removing HBase log directories from mounted volumes that may exist from a previous test run:"
echo "ACTIVE_CLUSTER_LOGS_DIR=${ACTIVE_CLUSTER_LOGS_DIR}"
echo "REPLICA_CLUSTER_LOGS_DIR=${REPLICA_CLUSTER_LOGS_DIR}"
rm -rf "${ACTIVE_CLUSTER_LOGS_DIR}" "${REPLICA_CLUSTER_LOGS_DIR}"
mkdir -p "${ACTIVE_CLUSTER_LOGS_DIR}" "${REPLICA_CLUSTER_LOGS_DIR}"
chmod 777 "${ACTIVE_CLUSTER_LOGS_DIR}" "${REPLICA_CLUSTER_LOGS_DIR}"

# Clone HBase source for Docker build context (Docker COPY doesn't follow symlinks)
echo "Cloning HBase source into ${REPLICA_DIR}/hbase for Docker build context..."
rm -rf "${REPLICA_DIR}/hbase"
git clone --local "${HBASE_ROOT}" "${REPLICA_DIR}/hbase"
rm -rf "${REPLICA_DIR}/hbase/.git"

cleanup() {
  local exit_code=$?
  if [ ${exit_code} -ne 0 ]; then
    echo "=== FAILURE ==="
    echo "An error occurred during this stage in the Jenkins run."
    echo "The HBase logs will be copied to: ${OUTPUT_DIR}"
    echo "Check the Jenkins run's Build Artifacts on the Status page."
  fi
  echo "=== Cleanup: Copying HBase logs to ${OUTPUT_DIR} ==="
  mkdir -p ${OUTPUT_DIR}/hbase-docker-logs ${OUTPUT_DIR}/hbase-docker-2-logs || true
  cp -r ${ACTIVE_CLUSTER_LOGS_DIR}/*log   ${OUTPUT_DIR}/hbase-docker-logs    || true
  cp -r ${REPLICA_CLUSTER_LOGS_DIR}/*log  ${OUTPUT_DIR}/hbase-docker-2-logs  || true
  echo "Logs can be found with the Jenkins run's Build Artifacts on the Status page"
  echo "=== Cleanup: Stopping Docker containers ==="
  docker compose -f "${DOCKER_COMPOSE_FILE}" down 2>/dev/null || true
  echo "=== Cleanup: Removing Docker image: ${HBASE_IMAGE} ==="
  docker rmi --force "${HBASE_IMAGE}" 2>/dev/null || true
  rm -rf "${REPLICA_DIR}/hbase"
  exit "${exit_code}"
}
trap cleanup EXIT

# Copy latest proto file from source
echo "Copying latest version of ActiveClusterSuffix.proto to $(pwd)/python/proto/"
cp "${HBASE_ROOT}/hbase-protocol-shaded/src/main/protobuf/server/ActiveClusterSuffix.proto" \
   python/proto/

export PYTHONPATH="$(pwd)"
echo "Set PYTHONPATH=${PYTHONPATH}"

# Create Python environment
echo "Creating Python environment: .venv"
python3 -m venv .venv
source .venv/bin/activate

# Install Python dependencies
echo "Installing Python libraries"
pip install --upgrade pip
pip install -r requirements.txt

# Compile protobuf
echo "Compiling Protobuf"
python3 python/proto/proto_compiler.py

# Build Docker images
echo "Building hbase-docker image"
./build-images.sh

# Run read-replica integration test suite
echo "Starting read-replica integration test scripts"
echo "Starting read-replica integration test suite via Pytest..."
pytest --html="${OUTPUT_DIR}/pytest-report.html" \
       --self-contained-html \
       --junitxml="${OUTPUT_DIR}/pytest-results.xml" \
       python/test/test_read_replica_feature.py

echo "=== Success: All read-replica integration tests passed. ==="

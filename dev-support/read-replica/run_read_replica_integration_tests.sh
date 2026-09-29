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

# run_read_replica_integration_tests.sh
#
# Inner test runner executed INSIDE the Docker container built from
# hbase/dev-support/docker/Dockerfile. In a typical test run, this
# script is invoked by hbase_nightly_read_replica_test.sh. It can be
# run on its own as well as long as it is done within the container
# mentioned above.
#
# What it does:
#   1. Clones the HBase source tree into a local directory for Docker build context
#   2. Copies and compiles the protobuf definitions needed by the Python tests
#   3. Creates a Python virtual environment and installs dependencies
#   4. Builds a Docker image for the active and replica clusters in a read-replica
#      setup
#   5. Runs the pytest read-replica integration suite using these clusters
#
# For usage information, run: ./run_read_replica_integration_tests.sh --help
set -e

REPLICA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${REPLICA_DIR}/output}"
export HBASE_ROOT="$(cd "${REPLICA_DIR}/../.." && pwd)"

export HBASE_IMAGE="hbase-read-replica:${BUILD_NUMBER:-local}"

JAVA_VERSION=17
KEEP_IMAGE=false
KEEP_CONTAINERS=false
PYTEST_K_VALUE=""

print_usage() {
  SCRIPT=$(basename "${BASH_SOURCE}")

  cat << __EOF

run_read_replica_integration_tests.sh

Inner test runner executed inside the Docker container built from
hbase/dev-support/docker/Dockerfile. Clones the HBase source, builds Docker
images for the active and read-replica clusters, and runs the pytest
integration suite.

This script is normally invoked by hbase_nightly_read_replica_test.sh, but
it can also be run on its own within the container mentioned above.

Usage: ${SCRIPT} [options]

  -h | --help                  Show this help message and exit.
  -j | --java-version <ver>    JVM version to use (default: 17). Sets JAVA_HOME
                                to /usr/lib/jvm/java-<ver>.
  -i | --keep-image            Do not remove the Docker image on exit.
  -c | --keep-containers       Do not run 'docker compose down' on exit.
  -k <expression>              Pytest -k filter expression for test selection.
                                (See Pytest documentation on -k for more info.)

__EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_usage
      exit 0
      ;;
    -j|--java-version)
      if [[ -n "$2" && "$2" != -* ]]; then
        JAVA_VERSION="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        print_usage >&2
        exit 1
      fi
      ;;
    -i|--keep-image)
      KEEP_IMAGE=true
      shift
      ;;
    -c|--keep-containers)
      KEEP_CONTAINERS=true
      shift
      ;;
    -k)
      if [[ -n "$2" && "$2" != -* ]]; then
        PYTEST_K_VALUE="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        print_usage >&2
        exit 1
      fi
      ;;
    *)
      echo "Unknown option: $1" >&2
      print_usage >&2
      exit 1
      ;;
  esac
done

# Set JAVA_HOME and update PATH for the selected JVM (e.g. 8, 11, 17, 21)
# MAVEN_HOME is set in hbase/dev-support/docker/Dockerfile
export JAVA_HOME="/usr/lib/jvm/java-${JAVA_VERSION}"
export PATH="${JAVA_HOME}/bin:${MAVEN_HOME}/bin:${PATH}"

echo "Using JAVA_HOME=${JAVA_HOME}"

echo "=== Inside Dev Container Environment ==="
echo "Replica dir: ${REPLICA_DIR}"
echo "Output dir: ${OUTPUT_DIR}"
echo "HBase root: ${HBASE_ROOT}"

echo "Changing to replica dir: ${REPLICA_DIR}"
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

# Clone HBase source for Docker build context (Docker COPY doesn't follow symlinks)
echo "Cloning HBase source into ${REPLICA_DIR}/hbase for Docker build context..."
rm -rf "${REPLICA_DIR}/hbase"
git clone --local "${HBASE_ROOT}" "${REPLICA_DIR}/hbase"
rm -rf "${REPLICA_DIR}/hbase/.git"

cleanup() {
  local exit_code=$?
  if [ ${exit_code} -ne 0 ]; then
    echo "=== FAILURE ==="
    echo "An error occurred during this stage of the run."
  fi
  if [ "${KEEP_CONTAINERS}" = "false" ]; then
    echo "=== Cleanup: Stopping Docker containers ==="
    docker compose -f "${DOCKER_COMPOSE_FILE}" down 2>/dev/null || true
  else
    echo "=== Cleanup: Keeping Docker containers (--keep-containers) ==="
  fi
  if [ "${KEEP_IMAGE}" = "false" ]; then
    echo "=== Cleanup: Removing Docker image: ${HBASE_IMAGE} ==="
    docker rmi --force "${HBASE_IMAGE}" 2>/dev/null || true
  else
    echo "=== Cleanup: Keeping Docker image: ${HBASE_IMAGE} (--keep-image) ==="
  fi
  echo "=== Cleanup: Deleting cloned HBase directory: ${REPLICA_DIR}/hbase ==="
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
PYTEST_K_ARGS=()
if [[ -n "${PYTEST_K_VALUE}" ]]; then
  PYTEST_K_ARGS=(-k "${PYTEST_K_VALUE}")
fi

echo "Starting read-replica integration test suite via Pytest..."
pytest -o log_cli=true --log-cli-level=INFO \
       --log-cli-format='%(asctime)s %(levelname)-5s %(module)s.%(funcName)s(%(lineno)d): %(message)s' \
       --log-cli-date-format='%Y-%m-%d %H:%M:%S' \
       --html="${OUTPUT_DIR}/read-replica-nightly-test-report.html" \
       --self-contained-html \
       --junitxml="${OUTPUT_DIR}/read-replica-nightly-test-results.xml" \
       python/test/test_read_replica_feature.py \
       "${PYTEST_K_ARGS[@]}"

echo "=== Success: All read-replica integration tests passed. ==="

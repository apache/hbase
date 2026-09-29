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

# hbase_nightly_read_replica_test.sh
#
# Outer driver script for the HBase read-replica integration test suite.
# Builds a dev-support Docker container image (from
# hbase/dev-support/docker/Dockerfile) using the HBase source tree as
# build context, then either:
#
#   - Launches run_read_replica_integration_tests.sh inside that
#     container to execute the full test suite (default), or
#   - Starts a long-lived dev container for interactive exploration
#     (-d|--dev mode).
#
# The container uses Docker-outside-of-Docker (DooD) by bind-mounting
# the host's Docker socket, and mounts .m2 for Maven cache reuse
# (override the default $HOME location with -m|--m2).
#
# For usage information, run: ./hbase_nightly_read_replica_test.sh --help
set -e

REPLICA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export HBASE_ROOT="$(cd "${REPLICA_DIR}/../.." && pwd)"

DEV_IMAGE_NAME="hbase-dev-support:${BUILD_NUMBER:-local}"

PYTEST_K_VALUE=""
JAVA_VERSION=""
M2_DIR="${HOME}"
DEV_MODE=false

print_usage() {
  SCRIPT=$(basename "${BASH_SOURCE}")

  cat << __EOF

hbase_nightly_read_replica_test.sh

Outer driver script for the HBase read-replica integration test suite. Builds a
dev-support Docker container image and either runs the inner test script
(run_read_replica_integration_tests.sh) inside it, or starts a long-lived dev
container for interactive exploration.

Usage: ${SCRIPT} [options]

  -h | --help                  Show this help message and exit.
  -d | --dev                   Start a detached dev container for interactive
                                exploration instead of running the test suite.
                                Prints the container ID and instructions for
                                entering and stopping it.
  -j | --java-version <ver>    JVM version forwarded to the inner test script
                                (run_read_replica_integration_tests.sh), which
                                uses it to set JAVA_HOME inside the container.
  -k <expression>              Pytest -k filter expression forwarded to the
                                inner test script for test selection.
  -m | --m2 <path>             Parent directory of the .m2 Maven cache to
                                bind-mount into the container. Defaults to
                                \$HOME. The directory <path>/.m2 will be
                                created if it does not exist.

__EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_usage
      exit 0
      ;;
    -d|--dev)
      DEV_MODE=true
      shift
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
    -m|--m2)
      if [[ -n "$2" && "$2" != -* ]]; then
        M2_DIR="$2"
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

echo "=== HBase Read-Replica Integration Test Driver ==="
echo "HBase Root: ${HBASE_ROOT}"
echo "Replica Dir: ${REPLICA_DIR}"
echo "Dev Container Image: ${DEV_IMAGE_NAME}"
echo "M2 Dir: ${M2_DIR}/.m2"

# Build the dev-support container image using HBASE_ROOT as the build context
echo "Building dev-support Docker image..."
docker build --platform linux/amd64 \
  -t "${DEV_IMAGE_NAME}" \
  -f "${HBASE_ROOT}/dev-support/docker/Dockerfile" \
  "${HBASE_ROOT}"

cleanup_host() {
  local exit_code=$?
  if [ -z "${BUILD_NUMBER}" ]; then
    echo "Local execution complete. Preserving local container image ${DEV_IMAGE_NAME}."
  else
    echo "Jenkins execution complete. Cleaning up image ${DEV_IMAGE_NAME}..."
    docker rmi --force "${DEV_IMAGE_NAME}" 2>/dev/null || true
  fi
  exit "${exit_code}"
}
trap cleanup_host EXIT

# Ensure host .m2 directory exists for caching
mkdir -p "${M2_DIR}/.m2"

if [ "${DEV_MODE}" = "true" ]; then
  # Start a detached dev container for interactive exploration
  echo "Starting dev container in background..."
  CONTAINER_ID=$(docker run -d \
    --platform linux/amd64 \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${HBASE_ROOT}:${HBASE_ROOT}" \
    -v "${M2_DIR}/.m2:/root/.m2" \
    -e OUTPUT_DIR="${OUTPUT_DIR}" \
    -e BUILD_NUMBER="${BUILD_NUMBER:-local}" \
    -w "${REPLICA_DIR}" \
    "${DEV_IMAGE_NAME}" \
    sleep infinity)

  echo ""
  echo "=== Dev container is ready ==="
  echo "Container ID: ${CONTAINER_ID}"
  echo "Image:        ${DEV_IMAGE_NAME}"
  echo ""
  echo "Enter the container:"
  echo ""
  echo "docker exec -it ${CONTAINER_ID} bash"
  echo ""
  echo "Stop and remove the container when done:"
  echo ""
  echo "docker stop ${CONTAINER_ID} && docker rm ${CONTAINER_ID}"
  echo ""
else
  JAVA_VERSION_ARGS=()
  if [[ -n "${JAVA_VERSION}" ]]; then
    JAVA_VERSION_ARGS=(-j "${JAVA_VERSION}")
  fi

  PYTEST_K_ARGS=()
  if [[ -n "${PYTEST_K_VALUE}" ]]; then
    PYTEST_K_ARGS=(-k "${PYTEST_K_VALUE}")
  fi

  # Run the inner test script inside the dev-support container via DooD
  echo "Launching dev container and starting test suite..."
  docker run --rm \
    --platform linux/amd64 \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${HBASE_ROOT}:${HBASE_ROOT}" \
    -v "${M2_DIR}/.m2:/root/.m2" \
    -e OUTPUT_DIR="${OUTPUT_DIR}" \
    -e BUILD_NUMBER="${BUILD_NUMBER:-local}" \
    -w "${REPLICA_DIR}" \
    "${DEV_IMAGE_NAME}" \
    ./run_read_replica_integration_tests.sh "${JAVA_VERSION_ARGS[@]}" "${PYTEST_K_ARGS[@]}"
fi

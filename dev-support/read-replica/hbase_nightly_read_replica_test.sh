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

# Driver script: Builds dev-support environment container and runs read-replica tests within it.
set -e

REPLICA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export HBASE_ROOT="$(cd "${REPLICA_DIR}/../.." && pwd)"

DEV_IMAGE_NAME="hbase-dev-support:${BUILD_NUMBER:-local}"

PYTEST_K_VALUE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -k)
      if [[ -n "$2" && "$2" != -* ]]; then
        PYTEST_K_VALUE="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing"
        exit 1
      fi
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [-k <expression>]"
      exit 1
      ;;
  esac
done

echo "=== HBase Read-Replica Integration Test Driver ==="
echo "HBase Root: ${HBASE_ROOT}"
echo "Replica Dir: ${REPLICA_DIR}"
echo "Dev Container Image: ${DEV_IMAGE_NAME}"

# 1. Build the dev-support container image using HBASE_ROOT as the build context
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
mkdir -p "${HOME}/.m2"

PYTEST_K_ARGS=()
if [[ -n "${PYTEST_K_VALUE}" ]]; then
  PYTEST_K_ARGS=(-k "${PYTEST_K_VALUE}")
fi

# 2. Run the inner test script inside the dev-support container via DooD
echo "Launching dev container and starting test suite..."
docker run --rm \
  --platform linux/amd64 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "${HBASE_ROOT}:${HBASE_ROOT}" \
  -v "${HOME}/.m2:/root/.m2" \
  -e OUTPUT_DIR="${OUTPUT_DIR}" \
  -e BUILD_NUMBER="${BUILD_NUMBER:-local}" \
  -w "${REPLICA_DIR}" \
  "${DEV_IMAGE_NAME}" \
  ./run-read-replica-integration-tests.sh "${PYTEST_K_ARGS[@]}"

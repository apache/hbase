<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Read-Replica Integration Tests

Integration test framework for HBase's Read-Replica feature, driven by Jenkins and Docker.

## Why Docker?

HBase's `MiniHBaseCluster` cannot support multi-cluster Read-Replica testing because the
`META_TABLE_NAME` static variable is shared across JVMs (see HBASE-29691). This framework
sidesteps that limitation by running two fully isolated HBase clusters in Docker containers
that share a filesystem-based `hbase.rootdir`.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Jenkins Agent                       │
│                                                         │
│  ┌──────────────────┐         ┌──────────────────┐      │
│  │  hbase-docker    │         │  hbase-docker-2  │      │
│  │  (Active Cluster)│         │ (Replica Cluster)│      │
│  │                  │         │                  │      │
│  │  ZooKeeper       │         │  ZooKeeper       │      │
│  │  HMaster         │         │  HMaster         │      │
│  │  RegionServer    │         │  RegionServer    │      │
│  │                  │         │                  │      │
│  │  read-only=false │         │  read-only=true  │      │
│  │  suffix=(empty)  │         │  suffix=replica1 │      │
│  └────────┬─────────┘         └────────┬─────────┘      │
│           │                            │                │
│           └────────────┬───────────────┘                │
│                        │                                │
│              ┌─────────▼───────────┐                    │
│              │  Shared Data Store  │                    │
│              │  (hbase.rootdir)    │                    │
│              │                     │                    │
│              │  /data-store/hbase  │                    │
│              └─────────────────────┘                    │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │           Python Test Scripts                    │   │
│  │  (communicate via `docker exec` + HBase Shell)   │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

Both clusters mount the same `data-store/hbase` directory as their `hbase.rootdir`. The active
cluster writes data and the replica cluster reads it after explicit `refresh_meta` /
`refresh_hfiles` calls. Each cluster has its own ZooKeeper instance and distinct
`hbase.meta.table.suffix` to avoid meta table collisions.

## Directory Structure

```
read-replica/
├── .env                        # Environment variables (image name, ports, paths)
├── Dockerfile                  # Multi-stage build for the hbase-docker image
├── build-images.sh             # Builds the Docker image from the local HBase checkout
├── docker-compose.yml          # Defines the two container services
├── requirements.txt            # Python dependencies
├── cluster1/                   # Active cluster configuration
│   └── conf/
│       ├── hbase-site.xml      #   hbase.global.readonly.enabled=false
│       ├── log4j2.properties
│       └── zoo.cfg
├── cluster2/                   # Replica cluster configuration
│   └── conf/
│       ├── hbase-site.xml      #   hbase.global.readonly.enabled=true, suffix=replica1
│       ├── log4j2.properties
│       └── zoo.cfg
├── python/
│   ├── proto/
│   │   ├── ActiveClusterSuffix.proto    # Automatically copied to this location by
│   │   │                                # hbase_nightly_read_replica_test.sh
│   │   └── proto_compiler.py            # Compiles .proto files into python/proto/generated/
│   ├── scripts/
│   │   └── verify_hbase_start.py    # Standalone startup verification (not part of pytest suite)
│   ├── src/
│   │   ├── hbase_docker_client.py   # Core client — talks to containers via docker exec
│   │   ├── environment_loader.py    # Reads env vars with validation
│   │   ├── logger_config.py         # Logging setup
│   │   └── utils.py                 # Shared test utilities
│   └── test/
│       ├── test_read_replica_feature.py             # Pytest entry point (TestReadReplica class)
│       ├── test_dual_active_cluster_startup.py
│       ├── test_create_drop_behavior.py
│       ├── test_put_get_delete_behavior.py
│       ├── test_read_only_flag_flipping.py
│       ├── test_cannot_promote_second_active_cluster.py
│       └── test_bulkloaded_data_and_region_splits.py
└── utils/
    ├── bulkload.sh             # In-container script for ImportTsv + completebulkload
    └── tsv_generator.py        # Generates random TSV data for bulkloading
```

## CI: Jenkins Nightly Pipeline

**Files:**
- `dev-support/read-replica/Jenkinsfile` — pipeline definition (`hbase read-replica feature checks`)
- `dev-support/read-replica/hbase_nightly_read_replica_test.sh` — test driver script

### When It Runs

The read-replica tests run as their own standalone nightly pipeline on the `master` and `branch-3`
branches, separate from the main HBase nightly build.

### What the Test Driver Does

`hbase_nightly_read_replica_test.sh` is invoked by the Jenkins stage and performs these steps:

| # | Step | Description |
|---|------|-------------|
| 1 | Clone HBase source | `git clone --local` into `read-replica/hbase/` for the Docker build context (Docker COPY can't follow symlinks) |
| 2 | Source `.env` and clean old logs | Loads environment variables and removes log directories from prior runs |
| 3 | Register cleanup trap | On exit: runs `docker compose down` (unless `--keep-containers`), removes the Docker image (unless `--keep-image`), and deletes the cloned source |
| 4 | Copy Protobuf | Copies the latest `ActiveClusterSuffix.proto` from the source tree into `python/proto/` |
| 5 | Set up Python environment | Creates a venv, installs dependencies from `requirements.txt` |
| 6 | Compile Protobuf | Runs `python/proto/proto_compiler.py` |
| 7 | Build Docker image | Runs `build-images.sh` (Maven build + Docker multi-stage build) |
| 8 | Run test suite | Runs `pytest` on `python/test/test_read_replica_feature.py`, producing an HTML report and JUnit XML results (see [Test Suite](#test-suite) below) |

### On Failure

If any test fails, the build is marked `UNSTABLE` (not `FAILURE`) so that later result
publishing stages are not skipped. HBase logs from both containers are archived as Jenkins
build artifacts for debugging.

## Test Suite

Tests live in `python/test/` and are run via pytest through the `TestReadReplica` class in
`test_read_replica_feature.py`. Each test method delegates to a corresponding module's
`run_test()` function. The class is decorated with `@pytest.mark.flaky(reruns=2, reruns_delay=2)`,
so any failing test is automatically retried up to 2 times before being marked as failed.

| Test | What it verifies |
|------|-----------------|
| `test_dual_active_cluster_startup` | Exactly one of two active clusters fails to start with an error about another active cluster already existing |
| `test_create_drop_behavior` | `create` and `drop` are rejected on the replica; `refresh_meta` propagates DDL changes |
| `test_put_get_delete_behavior` | `put`, `delete`, and `flush` are rejected on the replica; data propagates after `refresh_hfiles` |
| `test_read_only_flag_flipping` | Clusters can swap roles (15 iterations); `active.cluster.suffix.id` protobuf file stays consistent |
| `test_cannot_promote_second_active_cluster` | Disabling read-only on the replica raises `ReadOnlyTransitionException` while an active cluster exists |
| `test_bulkloaded_data_and_region_splits` | `ImportTsv` + `completebulkload` works on active, is rejected on replica; region splits propagate |

To run a specific test:

```bash
pytest python/test/test_read_replica_feature.py::TestReadReplica::test_create_drop_behavior
```

### Test Results

Pytest produces two output files in the `output/` directory:

- **HTML report** (`read-replica-nightly-test-report.html`) — a self-contained HTML report
  published via `publishHTML` in the Jenkinsfile, viewable from the Jenkins build page
- **JUnit XML** (`read-replica-nightly-test-results.xml`) — parsed by Jenkins to display
  individual test results in the build UI

## Key Components

### HBaseDockerClient

`python/src/hbase_docker_client.py` — The core interface to HBase containers. It:

- Executes commands inside containers via `docker exec ... bash -c`
- Wraps HBase Shell commands with retry logic and timeout handling
- Provides assertion helpers (`assert_read_only_error_occurs`, `assert_table_row_count`, etc.)
- Manages cluster lifecycle (start/stop containers, wait for readiness)
- Manipulates `hbase-site.xml` to toggle `hbase.global.readonly.enabled` at runtime

### .env File

Defines environment variables consumed by Docker Compose, the build script, and Python tests:

- `HBASE_IMAGE` — Docker image tag
- `HBASE_DATA_STORE_ROOT` — Host path for the shared data store
- `DOCKER_COMPOSE_FILE` — Absolute path to `docker-compose.yml`

### Protobuf Verification

The `ActiveClusterSuffix.proto` message defines the format of the `active.cluster.suffix.id`
file written to the shared data store. Tests compile this proto and deserialize the file to
verify that the recorded active cluster matches the expected configuration after role swaps.

## Running Locally

The easiest way to run the tests locally is with `hbase_nightly_read_replica_test.sh`, which
handles environment setup, Docker image builds, and pytest execution automatically. This is also
useful for reproducing test failures seen in CI.

```bash
# From the repo root — run the full suite
dev-support/read-replica/hbase_nightly_read_replica_test.sh
```

The script accepts two flags for local debugging:

| Flag | Description |
|------|-------------|
| `-i` / `--keep-image` | Preserves the Docker image after tests finish. This lets you spin up fresh containers with `docker compose up` from the `dev-support/read-replica/` directory without rebuilding the image. |
| `-c` / `--keep-containers` | Preserves the running containers after tests finish. This lets you `docker exec` into the containers to inspect state, logs, and HBase Shell. |

```bash
# Keep the image and containers for debugging
dev-support/read-replica/hbase_nightly_read_replica_test.sh --keep-image --keep-containers
```

### Running Manually

If you prefer to run the setup steps yourself (e.g. to skip the Docker image rebuild after the
first run):

```bash
cd dev-support/read-replica

# Override CI-specific paths
export HBASE_ROOT="$(cd ../.. && pwd)"

# Load remaining variables
set -a && source .env && set +a

# Set Python path
export PYTHONPATH="$(pwd)"

# Install Python dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Compile protobuf
cp "${HBASE_ROOT}/hbase-protocol-shaded/src/main/protobuf/server/ActiveClusterSuffix.proto" python/proto/
python3 python/proto/proto_compiler.py

# Build Docker image (requires Maven + Docker — skip if reusing a previous image)
./build-images.sh

# Run the full test suite
pytest python/test/test_read_replica_feature.py

# Or run a single test
pytest python/test/test_read_replica_feature.py::TestReadReplica::test_create_drop_behavior

# Clean up
docker compose -f docker-compose.yml down
```

### Mounted Volumes

The data-store directory (`tmp-read-replica-data/`) and log directories (`cluster1/logs/`,
`cluster2/logs/`) are mounted into the containers. Between `docker compose down` and
`docker compose up`, consider removing these directories to start with a clean state:

```bash
rm -rf tmp-read-replica-data cluster1/logs cluster2/logs
```

The shell script automatically cleans the log directories on each run, but the data-store
directory persists across runs.

**Prerequisites:** Docker, Docker Compose, Python 3, Maven, JDK 17.

## Related

- **JIRA:** [HBASE-30087](https://issues.apache.org/jira/browse/HBASE-30087)
- **Read-Replica feature PR:** [#8364](https://github.com/apache/hbase/pull/8364)
- **Background (MiniHBaseCluster limitation):** [HBASE-29691](https://issues.apache.org/jira/browse/HBASE-29691)

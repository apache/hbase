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
static `META_TABLE_NAME` variable is shared across JVMs (see HBASE-29691). This framework
sidesteps that limitation by running two fully isolated HBase clusters in Docker containers
that share a filesystem-based `hbase.rootdir`.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                   Host (Jenkins Agent or Local)                      │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │         Test Environment Container (dev-support image)         │  │
│  │   Built from: dev-support/docker/Dockerfile                    │  │
│  │   Contains: JDK, Maven, Python, Docker CLI                     │  │
│  │   Entry: run_read_replica_integration_tests.sh                 │  │
│  │                                                                │  │
│  │   ┌──────────────────┐         ┌──────────────────┐            │  │
│  │   │  hbase-docker    │         │  hbase-docker-2  │            │  │
│  │   │  (Active Cluster)│         │ (Replica Cluster)│            │  │
│  │   │                  │         │                  │            │  │
│  │   │  ZooKeeper       │         │  ZooKeeper       │            │  │
│  │   │  HMaster         │         │  HMaster         │            │  │
│  │   │  RegionServer    │         │  RegionServer    │            │  │
│  │   │                  │         │                  │            │  │
│  │   │  read-only=false │         │  read-only=true  │            │  │
│  │   │  suffix=(empty)  │         │  suffix=replica1 │            │  │
│  │   └────────┬─────────┘         └────────┬─────────┘            │  │
│  │            │                            │                      │  │
│  │            └────────────┬───────────────┘                      │  │
│  │                         │                                      │  │
│  │               ┌─────────▼───────────┐                          │  │
│  │               │  Shared Data Store  │                          │  │
│  │               │  (hbase.rootdir)    │                          │  │
│  │               │                     │                          │  │
│  │               │  /data-store/hbase  │                          │  │
│  │               └─────────────────────┘                          │  │
│  │                                                                │  │
│  │   ┌──────────────────────────────────────────────────────┐     │  │
│  │   │           Python Test Scripts                        │     │  │
│  │   │  (communicate via `docker exec` + HBase Shell)       │     │  │
│  │   └──────────────────────────────────────────────────────┘     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Docker socket bind-mounted into the test environment container      │
│  (Docker-outside-of-Docker / DooD)                                   │
└──────────────────────────────────────────────────────────────────────┘
```

The test environment container is built from `dev-support/docker/Dockerfile` and provides
JDK, Maven, Python, and the Docker CLI. It uses Docker-outside-of-Docker (DooD) by
bind-mounting the host's Docker socket, so the HBase cluster containers run as siblings on
the host's Docker daemon.

Both HBase clusters mount the same `data-store/hbase` directory as their `hbase.rootdir`.
The active cluster writes data and the replica cluster reads it after explicit `refresh_meta`
/ `refresh_hfiles` calls. Each cluster has its own ZooKeeper instance and distinct
`hbase.meta.table.suffix` to avoid meta table collisions.

## Directory Structure

```
read-replica/
├── .env                        # Environment variables (image name, ports, paths)
├── Dockerfile                  # Multi-stage build for the hbase-docker image
├── build-images.sh             # Builds the Docker image from the local HBase checkout
├── docker-compose.yml          # Defines the two container services
├── requirements.txt            # Python dependencies
├── hbase_nightly_read_replica_test.sh   # Outer driver script (runs on host)
├── run_read_replica_integration_tests.sh # Inner test runner (runs inside container)
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
│   │   │                                # run_read_replica_integration_tests.sh
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

## Scripts

The test framework uses two scripts in a layered architecture:

### `hbase_nightly_read_replica_test.sh` (Outer Driver)

Runs on the host (Jenkins agent or local machine). It builds a test environment Docker image
from `dev-support/docker/Dockerfile`, starts a container, and either runs the inner test
script automatically or drops into an interactive shell for development.

| Step | Description |
|------|-------------|
| 1 | Build the test environment Docker image (`hbase-dev-support:<build-number>`) |
| 2 | Start a container from that image, mounting the HBase repo, Docker socket, and `.m2` cache |
| 3 | Run `run_read_replica_integration_tests.sh` inside the container (or start an interactive shell in dev mode) |

**Options:**

| Flag | Description |
|------|-------------|
| `-d` / `--dev` | Start a detached dev container for interactive exploration instead of running the test suite. See [Dev Mode](#dev-mode). |
| `-k <expression>` | Pytest `-k` filter expression forwarded to the inner test script for test selection. See [Running Specific Tests](#running-specific-tests). |
| `-m` / `--m2 <path>` | Parent directory of the `.m2` Maven cache to bind-mount into the container. Defaults to `$HOME`. The directory `<path>/.m2` will be created if it does not exist. |
| `-j` / `--java-version <ver>` | JVM version forwarded to the inner test script, which uses it to set `JAVA_HOME` inside the container. |

### `run_read_replica_integration_tests.sh` (Inner Test Runner)

Runs inside the test environment container. Normally invoked by the outer driver, but can
also be run directly when using dev mode.

| Step | Description |
|------|-------------|
| 1 | Rsync a trimmed HBase tree into `read-replica/hbase/` for the Docker build context (excludes `.git`, `target/`, nested `read-replica/hbase`, etc.) |
| 2 | Copy and compile the `ActiveClusterSuffix.proto` protobuf definition |
| 3 | Create a Python virtual environment and install dependencies |
| 4 | Build the `hbase-read-replica` Docker image for the active and replica clusters |
| 5 | Run the pytest integration suite |

**Options:**

| Flag | Description |
|------|-------------|
| `-i` / `--keep-image` | Do not remove the `hbase-read-replica` Docker image on exit. Useful for re-running tests without rebuilding. |
| `-c` / `--keep-containers` | Do not run `docker compose down` on exit. Lets you inspect container state after tests. |
| `-k <expression>` | Pytest `-k` filter expression for test selection. |
| `-j` / `--java-version <ver>` | JVM version to use (default: 17). Sets `JAVA_HOME` to `/usr/lib/jvm/java-<ver>`. |

## CI: Jenkins Nightly Pipeline

**Files:**
- `dev-support/read-replica/Jenkinsfile` — pipeline definition (`hbase read-replica feature checks`)
- `dev-support/read-replica/hbase_nightly_read_replica_test.sh` — outer driver script
- `dev-support/read-replica/run_read_replica_integration_tests.sh` — inner test runner

### When It Runs

The read-replica tests run as their own standalone nightly pipeline on the `master` and `branch-3`
branches, separate from the main HBase nightly build.

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
The `ActiveClusterSuffix.proto` file is copied directly from the HBase repo during runtime
of `run_read_replica_integration_tests.sh`.

## Running Locally

The same script used by Jenkins works locally. The only prerequisite is Docker — the test
environment container provides JDK, Maven, Python, and everything else needed to build and
test.

### Running the Full Suite

From the repo root:

```bash
bash dev-support/read-replica/hbase_nightly_read_replica_test.sh
```

When run locally (no `BUILD_NUMBER` environment variable), the test environment image is
preserved so subsequent runs skip the image build. In Jenkins, the image is cleaned up
automatically.

### Running Specific Tests

Use `-k` to pass a pytest `-k` filter expression that selects tests by name
(see [pytest docs](https://docs.pytest.org/en/stable/example/markers.html#using-k-expr-to-select-tests-based-on-their-name) for details):

```bash
# Run two specific tests
bash dev-support/read-replica/hbase_nightly_read_replica_test.sh \
  -k "test_create_drop_behavior or test_put_get_delete_behavior"

# Run a single test
bash dev-support/read-replica/hbase_nightly_read_replica_test.sh \
  -k "test_put_get_delete_behavior"
```

### Dev Mode

Dev mode (`-d` / `--dev`) builds and starts the test environment container but does **not**
run the test suite. This gives you an interactive environment where you can run the inner
test script as many times as you need — useful for reproducing bugs and iterating on test
development.

**1. Start the dev container:**

```bash
bash dev-support/read-replica/hbase_nightly_read_replica_test.sh -d
```

The script prints the container ID and instructions for entering it.

**2. Enter the container:**

```bash
docker exec -it <container-id> bash
```

**3. Run the tests inside the container:**

```bash
# Run the full suite
bash run_read_replica_integration_tests.sh

# Run specific tests
bash run_read_replica_integration_tests.sh \
  -k "test_create_drop_behavior or test_put_get_delete_behavior"
```

**4. Speed up re-runs with `--keep-image`:**

By default, the inner script removes the `hbase-read-replica` cluster Docker image on exit.
Pass `--keep-image` to preserve it, so subsequent runs skip the Maven build and image
creation — saving significant time during development:

```bash
bash run_read_replica_integration_tests.sh --keep-image

# Combine with -k to iterate on a specific test
bash run_read_replica_integration_tests.sh --keep-image \
  -k "test_create_drop_behavior"
```

**5. Stop and remove the dev container when done:**

```bash
docker stop <container-id> && docker rm <container-id>
```

### Mounted Volumes

The data-store directory (`tmp-read-replica-data/`) is mounted into the containers. Between
`docker compose down` and `docker compose up`, consider removing this directory to start with
a clean state:

```bash
rm -rf tmp-read-replica-data
```

**Prerequisites:** Docker.

## Related

- **JIRA:** [HBASE-30087](https://issues.apache.org/jira/browse/HBASE-30087)
- **Read-Replica feature PR:** [#8364](https://github.com/apache/hbase/pull/8364)
- **Background (MiniHBaseCluster limitation):** [HBASE-29691](https://issues.apache.org/jira/browse/HBASE-29691)

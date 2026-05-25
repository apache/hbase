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

# AGENTS.md

Apache HBase is a distributed, scalable big data store built on HDFS and cloud
object storage.

## Repo Structure

This is a multi-module Maven project. Modules live in arbitrarily nested
folders; enumerate them by searching for `pom.xml` files (excluding `target/`
directories). The root `pom.xml` defines the full reactor and build order.
Note that some directories from removed or merged modules (e.g.,
`hbase-hadoop2-compat/`, `hbase-protocol/`, `hbase-rsgroup/`) may still exist
as empty shells with only `target/` remnants. If a directory has no `pom.xml`,
it is not part of the active build.

### Client and Server

The fundamental divide in this codebase is client-side vs. server-side, with
several modules shared between them.

- `hbase-client` -- The client library. Builds RPC requests, handles retries,
  manages connections. This is the public API that external consumers depend on.
- `hbase-server` -- RegionServer and Master implementations. Processes RPCs,
  manages regions, stores data. The largest module by far.
- Shared modules like `hbase-common`, `hbase-protocol-shaded`, and
  `hbase-metrics-api` are dependencies of both sides.

When orienting on unfamiliar code, first determine which side of this divide
you are on.

### Module Roles

**Core data path:**
`hbase-client` -> `hbase-server` (via protobuf RPCs defined in
`hbase-protocol-shaded`)

**Gateways** (alternative client entry points):
`hbase-rest` (HTTP/JSON), `hbase-thrift` (Thrift RPC)

**Coprocessors** are HBase's server-side extension framework. They allow custom
code to run inside RegionServer and Master processes, with the same privileges
as the host process. The base `Coprocessor` interface lives in `hbase-client`;
observer and endpoint interfaces (`RegionObserver`, `MasterObserver`, etc.) live
in `hbase-server`. Endpoint implementations live in `hbase-endpoint`. The
built-in `AccessController` coprocessor enforces ACLs; `VisibilityController`
enforces cell-level visibility labels. Third-party coprocessors are loaded via
configuration or table schema.

**Server subsystems** (separated from hbase-server for modularity):
`hbase-balancer`, `hbase-procedure`, `hbase-replication`, `hbase-asyncfs`,
`hbase-zookeeper`, `hbase-http`

**Shared libraries:**
`hbase-common`, `hbase-metrics` + `hbase-metrics-api`, `hbase-logging`,
`hbase-hadoop-compat`

**Extensions:**
`hbase-extensions` (currently `hbase-openssl` for native TLS support)

**Storage codecs:**
`hbase-compression/*` (pluggable algorithms), `hbase-external-blockcache`

**Packaging and shading:**
`hbase-shaded/*`, `hbase-assembly*`, `hbase-resource-bundle`

**Tooling:**
`hbase-shell` (JRuby REPL), `hbase-hbtop`, `hbase-mapreduce`, `hbase-backup`,
`hbase-diagnostics`

**Build infrastructure** (ignore for code tasks):
`hbase-build-configuration`, `hbase-checkstyle`, `hbase-annotations`,
`hbase-archetypes/*`, `hbase-dev-generate-classpath`

**Testing:**
`hbase-testing-util`, `hbase-it`, `hbase-examples`

### Navigating with @InterfaceAudience

Classes are annotated with `@InterfaceAudience` to indicate their intended
consumer:

- `Public` -- Stable client API. External consumers depend on these.
- `LimitedPrivate` -- Internal API shared across modules, scoped to a named
  audience (e.g., `COPROC`, `CONFIG`, `REPLICATION`, `AUTHENTICATION`). The
  audience name tells you who is expected to call this code.
- `Private` -- Module-internal. Not API.

These annotations are the fastest way to determine whether a class is part of
the external surface or internal plumbing.

### Key Entry Points

When investigating a behavior, start from where it enters the system:

- **Client RPCs**: `RSRpcServices` (RegionServer) and `MasterRpcServices`
  (Master) handle all client-initiated RPCs. Trace from the method matching
  the RPC name.
- **REST gateway**: resource classes in `hbase-rest` map HTTP verbs to
  operations.
- **Thrift gateway**: handler classes in `hbase-thrift` map Thrift methods.
- **Coprocessor hooks**: observer interfaces (`RegionObserver`,
  `MasterObserver`, etc.) define extension points. Implementations are loaded
  via configuration or table schema.
- **Procedures**: `hbase-procedure` defines the framework; concrete procedures
  (table create, region split, etc.) live in `hbase-server`.
- **Configuration**: properties are defined in `hbase-default.xml` (in
  `hbase-common`) and overridden by operators in `hbase-site.xml`.
- **Wire format**: `.proto` files in `hbase-protocol-shaded` define every RPC
  request/response and all persisted data structures. (Older branches had a
  separate `hbase-protocol` module; it has been removed on master.)

### Split Packages

The same Java package often appears in multiple modules (e.g., the
`coprocessor` package exists in `hbase-client`, `hbase-server`,
`hbase-endpoint`, and `hbase-examples`). Each module contributes different
classes to the package. When searching for a class, check which module it
lives in -- the module determines the execution context.

### Related Repositories

[hbase-thirdparty](https://github.com/apache/hbase-thirdparty) is a companion
project that patches and shades key dependencies (protobuf, netty, gson, etc.)
so that HBase's internal use of these libraries does not conflict with
versions on the application classpath. The `hbase-shaded-*` artifacts from
that repo appear as dependencies throughout this project's `pom.xml`. Changes
to shaded dependency versions or patches happen in that repo, not here.

### Developer Tooling

`dev-support/` contains CI configuration, release automation, code analysis
scripts, and other maintainer tools. PR-level CI has migrated to GitHub
Actions (`.github/workflows/`), but nightly and branch-level CI still runs
via configurations in `dev-support/`. That directory also holds release
scripts, docker-based test environments, and various developer utilities.
See `dev-support/README.md` for a full index.

`conf/` holds default configuration templates (`hbase-site.xml`,
`hbase-env.sh`, `log4j2.properties`). `bin/` holds shell scripts for cluster
lifecycle and operations.

`dev-support/design-docs/` collects design documents and proposals for major
features. These capture the rationale behind complex subsystems and are useful
for understanding why the code is structured the way it is.

### Conventions

- Tests mirror source paths: `src/test/java` parallels `src/main/java`
- Generated code (protobuf, etc.) lives in `target/` and is not checked in
- Configuration properties use `hbase.` prefix
- The shell is JRuby wrapping the Java client API

## Documentation

The project website (https://hbase.apache.org) is maintained in this repo under
`hbase-website/`. User-facing and administrator-facing documentation covering
configuration, security, architecture, schema design, operations, APIs, and
more lives in `hbase-website/app/pages/_docs/docs/_mdx/`. The table of
contents and page ordering is defined in the `meta.json` files within that
tree.

The site also serves https://hbase.apache.org/llms-full.txt, which
concatenates all documentation pages into a single text file suitable for
LLM context ingestion.

## Security Model

The project's security model is documented at
`hbase-website/app/pages/_landing/security-model/content.md`
(published at https://hbase.apache.org/security-model).
Read that document for the full security model including trust boundaries,
what constitutes a valid vulnerability, and what does not.

When performing security analysis of this codebase, use the navigation
structure above to determine the role of the code under review, then apply
the security model to interpret findings in context.

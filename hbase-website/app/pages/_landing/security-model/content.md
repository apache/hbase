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

# Security Model

This page describes the security model of Apache HBase. It details the assumptions and guarantees the project makes with respect to security. It is intended to help operators deploy HBase safely, to help security researchers understand what constitutes a legitimate vulnerability, and to help the [Apache Security Team](https://www.apache.org/security/) efficiently triage incoming reports.

This page was created following the [ASF recommendation for documenting project security models](https://cwiki.apache.org/confluence/display/SECURITY/Documenting+your+security+model).

## Reporting Security Vulnerabilities

To report an undisclosed, sensitive security vulnerability in Apache HBase, please preferentially send your report to the private mailing list [private@hbase.apache.org](mailto:private@hbase.apache.org). You may also report security vulnerabilities to the Apache Software Foundation's security team privately via email at [security@apache.org](mailto:security@apache.org). Please do not use JIRA or any public channel for security reports.

HBase follows the [Apache Software Foundation's vulnerability handling policy](https://www.apache.org/security/).

## Assumption: Operator-Secured Production Deployments

HBase requires operators to configure authentication and authorization for production deployments. This is the foundational assumption of the HBase security model.

HBase ships with a default configuration that does not enable authentication or authorization. This default exists solely to aid developers, testing, and CI/CD environments. It does not imply under any circumstances that deploying or running HBase without security is safe or desirable for production use.

No realistic production deployment runs HBase without security configured. The HBase documentation provides [comprehensive guidance on configuring security](/docs/security), including Kerberos authentication, SASL, Access Control Lists (ACLs), and visibility labels.

Vulnerability reports that assume or require an insecure configuration as part of an attack chain are not valid security reports. Such reports describe the expected behavior of an intentionally unsecured configuration, not a security flaw.

## Trust Boundaries

### Network Perimeter

HBase is designed for deployment within a trusted network perimeter, such as a private datacenter network or a properly configured cloud VPC. HBase services should not be directly exposed to the public internet.

Operators are responsible for ensuring appropriate network-level controls (firewalls, security groups, network segmentation) are in place around the HBase cluster.

### Cluster-Internal Trust

All HBase server-side processes, such as the HMaster and RegionServers, and their communication with ZooKeeper and HDFS, trust each other within a properly configured cluster. Compromising any one of these processes is effectively equivalent to compromising the entire cluster. These are all components of a single distributed system that must cooperate to function.

Inter-process authentication is enforced via Kerberos/SASL, ensuring only legitimate cluster members can participate.

### HDFS as Trusted Storage

HBase relies on HDFS or compatible distributed filesystem, or S3 or S3-alike cloud object storage, as its persistent storage layer. HBase assumes that the underlying storage layer access controls are correctly configured and that the storage layer is part of the same trust domain. An attacker with direct write access to the underlying storage layer can corrupt or manipulate HBase data regardless of HBase-level access controls.

### Client Trust Boundary

When authentication and authorization are configured:

- **Authentication** (Kerberos/SASL) verifies client identity before any operations are permitted.
- **Authorization** (ACLs) controls which authenticated users can perform which operations on which resources.
- Unauthenticated clients are rejected.

When authentication is not configured, which is only recommended for development or test environments, any client that can reach HBase over the network can perform any operation. This is expected and intentional for that configuration.

## Gateway Services

The REST and Thrift gateways are optional services that provide HTTP and Thrift protocol access to HBase. In their default configuration, these gateways do not perform authentication. For production use, operators must configure authentication on these gateways (SPNEGO/Kerberos for REST, SASL for Thrift) and/or restrict network access to them. See [Secure Client Access](/docs/security/client-access) for detailed configuration instructions. The gateways inherit the same trust model as the rest of HBase. When security is not configured, they are open. When security is configured, they enforce authentication and can support impersonation with proper proxy user setup. The REST gateway's `hbase.rest.readonly` flag and the Thrift gateway's security settings are operational controls for the administrator to configure appropriately.

Reports about unauthenticated access through gateways that have not been configured for authentication describe expected behavior, not a security flaw.

## Coprocessors

Coprocessors are a powerful extension mechanism that allows custom code to execute within HBase server processes. Loading a coprocessor is equivalent to granting full server-level access. Coprocessor code runs in the same JVM as the HBase server process with the same privileges. This is by design. Coprocessors are intended for trusted server-side extensions.
Operators control which coprocessors are loaded through server-side configuration (`hbase.coprocessor.region.classes`, `hbase.coprocessor.master.classes`, `hbase.coprocessor.regionserver.classes`) and/or through table descriptors. The [CoprocessorWhitelistMasterObserver](https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/security/access/CoprocessorWhitelistMasterObserver.html) can be used to restrict which coprocessor JARs may be loaded.

When authorization is configured, only users with appropriate permissions can modify table descriptors, and therefore only authorized users can affect coprocessor loading via table schema changes. The ability to load coprocessors via table descriptors is a feature. When access to table schema modification is restricted by ACLs, as it must be in production, this is not a vulnerability.

## Web UIs

The HBase server web UIs are administrative monitoring interfaces. They are designed for use within the trusted network and are helpful for operators and developers.

Information exposed through the web UIs, such as software version, configuration properties, table metadata, and operational metrics, is not considered sensitive within HBase's security model because these interfaces are expected to be accessible only within the trusted network perimeter.

The web UI can optionally be configured with SPNEGO authentication. See [Secure Access to HBase Web UIs](/docs/security/web-ui).

## What Is Considered a Vulnerability

The following categories of issues are considered valid security vulnerabilities and should be reported to [private@hbase.apache.org](mailto:private@hbase.apache.org) or [security@apache.org](mailto:security.apache.org):

- **Authentication bypass**: Circumventing configured Kerberos/SASL authentication to gain access without valid credentials.
- **Authorization bypass**: An authenticated user performing operations beyond their granted ACL permissions.
- **Privilege escalation**: An authenticated user with limited permissions gaining administrative or superuser capabilities.
- **Data corruption or loss**: Unauthorized modification or deletion of data by a user who should not have write access (when ACLs are configured).
- **Cross-user data access**: An authenticated user accessing another user's data in violation of configured ACLs or visibility labels.
- **Remote code execution**: Achieving arbitrary code execution by an authenticated, non-administrative user when coprocessor loading is properly restricted.

## What Is NOT Considered a Vulnerability

The following categories of reports do not constitute security vulnerabilities in Apache HBase:

- **Behavior in unsecured configurations**: Any issue that requires authentication and/or authorization to be unconfigured as a prerequisite. The default configuration is for development only, and running it in production is an operator error, not a vulnerability.
- **Unauthenticated access to unconfigured gateways**: The REST and Thrift gateways operating without authentication when authentication has not been configured.
- **Administrative actions by authorized administrators**: Superusers and global administrators can, by design, perform any operation in the cluster. This includes loading coprocessors, modifying table schemas, and accessing all data.
- **Access requiring operator-level cluster configuration changes**: Issues that require modifying `hbase-site.xml` or other server configuration files require host-level access, which is outside the HBase security boundary.
- **Information disclosure via administrative interfaces on trusted networks**: Version information, configuration details, or metrics visible in the web UIs when accessed from within the trusted network.
- **Exposure of services on untrusted networks**: Placing HBase services directly on the public internet without firewalls or VPN is an operator misconfiguration, not a vulnerability.

## Security Hardening

The HBase project welcomes reports about potential security hardening improvements, even when the behavior described falls outside the formal vulnerability criteria above. Such reports are valuable and will be considered for implementation as security improvements. Please use [JIRA](https://issues.apache.org/jira/browse/HBASE) for hardening suggestions (not the private security list), as these are by definition not vulnerability disclosures.

Examples of welcome hardening suggestions include:

- Improvements to default configurations that reduce risk of operator misconfiguration.
- Better input validation that, while not exploitable in a secured deployment, improves robustness

## Transport Security

Transport-level encryption (TLS/SSL) for RPC, web UIs, and gateways is supported and documented but is optional in the security model. Whether transport encryption is required depends on the network environment. Within a physically secured private datacenter, operators may reasonably choose not to encrypt intra-cluster traffic. In cloud environments or across network boundaries, transport encryption should be configured. See [TLS Configuration](/docs/security/tls) for details.

The decision to use or not use transport encryption is an operational choice that depends on    the deployment environment and does not affect the authentication and authorization requirements for production.

## Further Reading

- [Securing Apache HBase](/docs/security) — Comprehensive guide to configuring HBase security
- [Secure Client Access](/docs/security/client-access) — Kerberos, SASL, REST gateway, and Thrift gateway authentication
- [Securing Access to Your Data](/docs/security/data-access) — ACLs, visibility labels, and encryption at rest
- [ACL Matrix](/docs/acl-matrix) — Detailed mapping of HBase operations to required permissions
- [Security Configuration Example](/docs/security/example) — End-to-end example of a secure HBase configuration
- [Apache Software Foundation Security Policy](https://www.apache.org/security/) — ASF-wide security policy

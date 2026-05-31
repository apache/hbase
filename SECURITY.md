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

# Security Policy

## Reporting a Vulnerability

To report a security vulnerability in Apache HBase, please send a
private email to <security@apache.org> — the Apache Software
Foundation security team. Do **not** open a public GitHub issue or
pull request, as that risks disclosure before a fix is available.

The Apache HBase PMC is looped in privately as part of the standard
ASF disclosure process. See <https://www.apache.org/security/> for
the overall ASF vulnerability handling flow.

## Threat Model

Apache HBase's security model — trust boundaries, what constitutes a
valid vulnerability report, and what does not — is documented at:

  <https://hbase.apache.org/security-model/>

Please read that document before filing a report so the PMC can triage
quickly and accurately.

## Tooling notes

Agents and automated security tools analyzing this repository should
also consult [`AGENTS.md`](./AGENTS.md), which references the same
security model as the authoritative scope document.

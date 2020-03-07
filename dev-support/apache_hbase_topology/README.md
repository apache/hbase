<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# apache_hbase clusterdock topology

## Overview
*clusterdock* is a framework for creating Docker-based container clusters. Unlike regular Docker
containers, which tend to run single processes and then exit once the process terminates, these
container clusters are characterized by the execution of an init process in daemon mode. As such,
the containers act more like "fat containers" or "light VMs;" entities with accessible IP addresses
which emulate standalone hosts.

*clusterdock* relies upon the notion of a topology to define how clusters should be built into
images and then what to do with those images to start Docker container clusters.

## Usage
The *clusterdock* framework is designed to be run out of its own container while affecting
operations on the host. To avoid problems that might result from incorrectly
formatting this framework invocation, a Bash helper script (`clusterdock.sh`) can be sourced on a
host that has Docker installed. Afterwards, running any of the binaries intended to carry
out *clusterdock* actions can be done using the `clusterdock_run` command.
```
wget https://raw.githubusercontent.com/cloudera/clusterdock/master/clusterdock.sh
# ALWAYS INSPECT SCRIPTS FROM THE INTERNET BEFORE SOURCING THEM.
source clusterdock.sh
```

Since the *clusterdock* framework itself lives outside of Apache HBase, an environmental variable
is used to let the helper script know where to find an image of the *apache_hbase* topology. To
start a four-node Apache HBase cluster with default versions, you would simply run
```
CLUSTERDOCK_TOPOLOGY_IMAGE=apache_hbase_topology_location clusterdock_run \
    ./bin/start_cluster apache_hbase --secondary-nodes='node-{2..4}'
```

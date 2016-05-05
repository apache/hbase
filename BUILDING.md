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

# Building HBase native client

The HBase native client build using buck and produces a linux library.


# Dependencies

The easiest way to build hbase-native-client is to
use [Docker](https://www.docker.com/). This will mean that any platform
with docker can be used to build the hbase-native-client. It will also
mean that you're building with the same versions of all dependencies that
the client is tested with.

On OSX the current boot2docker solution will work however it's pretty
slow. If things get too slow using [dinghy](https://github.com/codekitchen/dinghy)
can help speed things up by using nfs.

If possible pairing virtual box with dinghy will result in the fastest,
most stable docker environment. However none of it is needed.

# Building using docker

To start with make sure that you have built the java project using
`mvn package -DskipTests`. That will allow all tests to spin up a standalone
hbase instance from the jar's created.

Then go into the hbase-native-client directory and run `./bin/start-docker.sh`
that will build the docker development environment and when complete will
drop you into a shell on a linux vm with all the tools needed installed.

# Buck

From then on we can use [buck](https://buckbuild.com/) to build everything.
For example:
```
buck build //core:core
buck test --all
buck build //core:simple-client
```

That will build the library, then build and test everything, then build
the simple-client binary. Buck will find all modules used, and compile
them in parallel, caching the results. Output from buck is in the buck-out
foulder. Generated binaries are in buck-out/gen logs are in buck-out/logs


# Make

If learning buck isn't your thing there is a Makefile wrapper for your
convenience.

```
make help
make check
make clean
make all
make build
```

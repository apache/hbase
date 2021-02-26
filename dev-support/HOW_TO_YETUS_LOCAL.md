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

# How To Use Apache Yetus with Apache HBase

This document describes how to get up and running with [Apache Yetus][yetus],
as pertains to the development of Apache HBase. Specifically this covers the use
of `test-patch`, of which HBase developers make use for tasks related to code
quality. These are normally run via automation in the foundation's
[Build][builds] infrastructure. They can also be run locally, which is the
subject of this document.

The Yetus project provides its own documentation of `test-patch` in
[Basic PreCommit][yetus-basic-precommit]. By comparison, this document is
intended to be highly abbreviated, hands-on, and focused on the HBase use-case.
See that document for more complete explanations and further details.

## Installation

In order to run Yetus, you'll need to first install Yetus and its dependencies.
This is somewhat simplified when used in Docker mode. Yetus can be retrieved
from a [distribution artifact][yetus-downloads]. Homebrew/Linuxbrew users can
install from the tap, the process for which is also described on the downloads
page.

# Usage Basics

Apache Yetus is comprised of a number of different components. The focus of our
interest is `test-patch`. `test-patch` is a modular system. Many modules depend
on some external tool to provide the underlying functionality. For example, the
`compile` check delegates to a number of provider modules, for example `maven`
or `gradle` for JVM projects. In order to use these modules, those tools must
be installed. Yetus calls these modules "plugins".

To see a list of all plugins available to `test-patch`, use

```shell script
$ test-patch --list-plugins
```

To see a list of all the options available in both the core modules as well as
all the plugins, use

```shell script
$ test-patch --plugins=all --help
```

An invocation of `test-patch` requires use of one or more plugins. Often times,
when the full suite of checks are run, specify the meta-plugin "all". Otherwise,
a limited selection of plugins can be selected using the `--plugins` argument.

## The Workspace, The Patch

`test-patch` operates within a "workspace," a checkout from a source control
repository. It has a number of options pertaining to this workspace, such as
the path to the workspace (`--basedir`) and whether it will permit the presence
of uncommitted changes therein (`--dirty-workspace`).

Onto this workspace, it can optionally apply a change, a.k.a., the "patch" in
"test patch." The patch can come from a number of sources, including a patch
file, a JIRA ID, a Pull Request, &c. Or, explicitly inform Yetus that no patch
file is provided, and the repository should be checked as is, by passing
`--empty-patch`.

## Personalities

`test-patch` is extremely extensible. Even the functionality of its core
modules can be extended or overridden. It allows for this type of
"personalization" by way of "personality" overrides. Yetus ships a number of
these personalities; a pre-packaged personality can be selected via the
`--project` parameter. There is a provided HBase personality in Yetus, however
the HBase project maintains its own within the HBase source repository. Specify
the path to the personality file using `--personality`. The HBase repository
places this file under `dev-support/hbase-personality.sh`. 

## Docker mode

Running Yetus in Docker mode simplifies the concerns of dependencies because
the provided `Dockerfile` handles their installation automatically. However,
for MacOS users, there are a number of known issues with running Docker on OSX,
so it may be preferable to instead run outside of the container.

To run in Docker, of course you must install `docker` or some container runtime
equivalent. [Docker Desktop][docker-desktop] is an option for Mac and Windows
users.

When running `test-patch` with HBase in docker mode, two parameters must be
provided. `--docker` activates the `docker` module, enabling the feature.
Additionally, `--dockerfile` points Yetus at HBase's provided `Dockerfile`,
instead of using the one provided out-of-the-box by Yetus. The HBase repository
places this file under `dev-support/docker/Dockerfile`.

## JVM and JAVA_HOME

HBase supports compilation and test execution on a number of different JVM
versions. To facilitate this, the `Dockerfile` installs multiple JVMs. An
invocation of `test-patch` requires additional parameters in order to specify
the target JVM. How you do this is slightly different based on whether you run
with or without Docker mode.

### Setting `JAVA_HOME` Outside of Docker Mode

Simply specify `JAVA_HOME` in the environment in which `test-patch` is launched.

### Setting `JAVA_HOME` Inside of Docker Mode

The docker image JDKs are available under `/usr/lib/jvm`. Symlinks are provided
for each supported major version, i.e., `java-11`. Use the `test-patch` argument
`--java-home`.

# Example: Run SpotBugs

One of the checks supported by Yetus is the static analysis tool
[SpotBugs][spotbugs]. Let's put together all the above and run SpotBugs with
`test-patch`.

The SpotBugs check depends on a SpotBugs installation. This is provided by the
docker file. For running without docker, you'll need to download and unpack the
SpotBugs binary distribution.

Running the SpotBugs check also depends on [Apache Maven][maven], the build
system used by HBase. In order for the check to function, both the `maven`
and `spotbugs` plugins must be specified. If this dependency is omitted, Yetus
will error with a message similar to

```
ERROR: you can't specify maven as the buildtool if you don't enable the plugin.
```

To run just the SpotBugs check, we must explicitly specify `spotbugs` and its
dependency using `--plugins=maven,spotbugs`.

## Without Docker

Putting it all together, without using docker:

```shell script
$ cd /path/to/hbase
$ JAVA_HOME=/path/to/jdk-8 test-patch.sh \
  --plugins=maven,spotbugs \
  --spotbugs-home=/path/to/spotbugs/
  --dirty-workspace \
  --empty-patch \
  --personality=./dev-support/hbase-personality.sh
```

## With Docker

Putting it all together, using docker:

```shell script
$ cd /path/to/hbase
$ test-patch.sh \
  --plugins=maven,spotbugs \
  --dirty-workspace \
  --empty-patch \
  --personality=./dev-support/hbase-personality.sh \
  --docker \
  --dockerfile=./dev-support/docker/Dockerfile \
  --java-home=/usr/lib/jvm/java-8
```

[builds]: https://builds.apache.org
[docker-desktop]: https://www.docker.com/products/docker-desktop
[maven]: https://maven.apache.org
[spotbugs]: https://spotbugs.github.io
[yetus]: https://yetus.apache.org
[yetus-basic-precommit]: https://yetus.apache.org/documentation/in-progress/precommit-basic/
[yetus-downloads]: https://yetus.apache.org/downloads/

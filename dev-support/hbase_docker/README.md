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

# hbase_docker

## Overview

The Dockerfile in this folder can be used to build a Docker image running
the latest HBase master branch in standalone mode. It does this by setting
up necessary dependencies, checking out the master branch of HBase from
GitHub, and then building HBase. By default, this image will start the HMaster
and launch the HBase shell when run.

## Usage

1. Download x64 .tar.gz files of the Oracle JDK and Apache Maven and place them
   in this folder (i.e. both tarballs must be in the same folder as the
   Dockerfile). Also note that the Dockerfile will properly pick up the tarballs
   as long as the JDK file has "jdk" in its name and the Maven file contains
   "maven". As an example, while developing this Dockerfile, my working directory
   looked like this:

   ```
   $ ls -lh
   total 145848
   -rw-r--r-- 1 root root   6956162 Sep  3 15:48 apache-maven-3.2.3-bin.tar.gz
   -rw-r--r-- 1 root root      2072 Sep  3 15:48 Dockerfile
   -rw-r--r-- 1 root root 142376665 Sep  3 15:48 jdk-7u67-linux-x64.tar.gz
   -rw-r--r-- 1 root root      1844 Sep  3 15:56 README.md
   ```
2. Ensure that you have a recent version of Docker installed from
   [docker.io](http://www.docker.io).
3. Set this folder as your working directory.
4. Type `docker build -t hbase_docker .` to build a Docker image called **hbase_docker**.
   This may take 10 minutes or more the first time you run the command since it will
   create a Maven repository inside the image as well as checkout the master branch
   of HBase.
5. When this completes successfully, you can run `docker run -it hbase_docker`
   to access an HBase shell running inside of a container created from the
   **hbase_docker** image. Alternatively, you can type `docker run -it hbase_docker
   bash` to start a container without a running HMaster. Within this environment,
   HBase is built in /root/hbase-bin.

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Image for building HBase releases. Based on Ubuntu 16.04.
#
# Includes:
# * Java 8
FROM ubuntu:18.04

# These arguments are just for reuse and not really meant to be customized.
ARG APT_INSTALL="apt-get install --no-install-recommends -y"

# Install extra needed repos and refresh.
#
# This is all in a single "RUN" command so that if anything changes, "apt update" is run to fetch
# the most current package versions (instead of potentially using old versions cached by docker).
RUN apt-get clean && \
  apt-get update && \
  # Install openjdk 8.
  $APT_INSTALL openjdk-8-jdk && \
  update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java && \
  # Install build / source control tools
  $APT_INSTALL curl gnupg python-pip wget git maven subversion lsof \
    libcurl4-openssl-dev libxml2-dev && \
  pip install python-dateutil

WORKDIR /opt/hbase-rm/output

ARG UID
RUN useradd -m -s /bin/bash -p hbase-rm -u $UID hbase-rm
USER hbase-rm:hbase-rm

ENTRYPOINT [ "/opt/hbase-rm/do-release.sh" ]

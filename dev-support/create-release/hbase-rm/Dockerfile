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

# Install extra needed repos and refresh.
#
# This is all in a single "RUN" command so that if anything changes, "apt update" is run to fetch
# the most current package versions (instead of potentially using old versions cached by docker).
RUN DEBIAN_FRONTEND=noninteractive apt-get -qq -y update \
  && DEBIAN_FRONTEND=noninteractive apt-get -qq -y install --no-install-recommends \
    curl=7.58.0-2ubuntu3.8 \
    git=1:2.17.1-1ubuntu0.5 \
    gnupg=2.2.4-1ubuntu1.2 \
    libcurl4-openssl-dev=7.58.0-2ubuntu3.8 \
    libxml2-dev=2.9.4+dfsg1-6.1ubuntu1.3 \
    lsof=4.89+dfsg-0.1 \
    maven=3.6.0-1~18.04.1 \
    openjdk-8-jdk=8u242-b08-0ubuntu3~18.04 \
    python-pip=9.0.1-2.3~ubuntu1.18.04.1 \
    subversion=1.9.7-4ubuntu1 \
    wget=1.19.4-1ubuntu2.2 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java \
  && pip install \
    python-dateutil==2.8.1

WORKDIR /opt/hbase-rm/output

ARG UID
RUN useradd -m -s /bin/bash -p hbase-rm -u $UID hbase-rm
USER hbase-rm:hbase-rm

ENTRYPOINT [ "/opt/hbase-rm/do-release.sh" ]

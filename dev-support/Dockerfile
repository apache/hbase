##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This Dockerfile is to setup environment for dev-support scripts which require
# dependencies outside of what Apache Jenkins machines may have.
#
# Specifically, it's used for the flaky test reporting job defined in
# dev-support/flaky-tests/flaky-reporting.Jenkinsfile
FROM ubuntu:22.04

COPY . /hbase/dev-support

RUN DEBIAN_FRONTEND=noninteractive apt-get -qq -y update \
    && DEBIAN_FRONTEND=noninteractive apt-get -qq -y install --no-install-recommends \
      curl='7.81.0-*' \
      python3-pip='22.0.2+dfsg-*' \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir -r /hbase/dev-support/python-requirements.txt

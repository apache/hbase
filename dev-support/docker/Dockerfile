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

# Dockerfile for installing the necessary dependencies for building Hadoop.
# See BUILDING.txt.

FROM maven:3.5-jdk-8

RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
       git \
       bats \
       findbugs \
       libperl-critic-perl \
       pylint \
       python-dateutil \
       rsync \
       make \
       gcc \
       libc6-dev \
       ruby \
       ruby-dev \
       shellcheck \
       wget \
       && \
    gem install rake rubocop ruby-lint

ENV FINDBUGS_HOME /usr

###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS -Xmx3g

CMD /bin/bash

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

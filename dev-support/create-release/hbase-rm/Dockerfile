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

# Image for building HBase releases. Based on Ubuntu 18.04.
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
    curl='7.58.0-*' \
    git='1:2.17.1-*' \
    gnupg='2.2.4-*' \
    libcurl4-openssl-dev='7.58.0-*' \
    libxml2-dev='2.9.4+dfsg1-*' \
    lsof='4.89+dfsg-*' \
    openjdk-8-jdk='8u*' \
    python-pip='9.0.1-*' \
    subversion='1.9.7-*' \
    wget='1.19.4-*' \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java \
  && pip install \
    python-dateutil==2.8.1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install mvn 3.6.3.
ARG MAVEN_VERSION=3.6.3
ARG SHA=c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries
RUN mkdir -p /opt/maven \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /opt/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /opt/maven/bin/mvn /usr/bin/mvn

# Install Apache Yetus
ENV YETUS_VERSION 0.12.0
RUN curl -L "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/yetus/${YETUS_VERSION}/apache-yetus-${YETUS_VERSION}-bin.tar.gz" | \
        tar xvz -C /opt
ENV YETUS_HOME /opt/apache-yetus-${YETUS_VERSION}

ARG UID
ARG RM_USER
RUN groupadd hbase-rm && \
    useradd --create-home --shell /bin/bash -p hbase-rm -u $UID $RM_USER && \
    mkdir /home/$RM_USER/.gnupg && \
    chown -R $RM_USER:hbase-rm /home/$RM_USER && \
    chmod -R 700 /home/$RM_USER

USER $RM_USER:hbase-rm
WORKDIR /home/$RM_USER/hbase-rm/

ENTRYPOINT [ "./do-release.sh" ]

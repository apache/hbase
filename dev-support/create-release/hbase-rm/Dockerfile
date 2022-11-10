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

# Image for building HBase releases. Based on Ubuntu 22.04.
FROM ubuntu:22.04

# Install extra needed repos and refresh.
#
# This is all in a single "RUN" command so that if anything changes, "apt update" is run to fetch
# the most current package versions (instead of potentially using old versions cached by docker).
RUN DEBIAN_FRONTEND=noninteractive apt-get -qq -y update \
  && DEBIAN_FRONTEND=noninteractive apt-get -qq -y install --no-install-recommends \
    curl='7.81.0-*' \
    git='1:2.34.1-*' \
    gnupg='2.2.27-*' \
    libcurl4-openssl-dev='7.81.0-*' \
    libxml2-dev='2.9.13+dfsg-*' \
    libxml2-utils='2.9.13+dfsg-*' \
    lsof='4.93.2+dfsg-*' \
    openjdk-8-jdk='8u*' \
    openjdk-11-jdk-headless='11*' \
    python3-pip='22.0.2+dfsg-*' \
    subversion='1.14.1-*' \
    wget='1.21.2-*' \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && pip3 install --no-cache-dir python-dateutil==2.8.2

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install JEnv
# update{-java}-alternatives is a global decision requiring sudo, but specifying a JDK at runtime
# requires an update for the local user/shell. Use JEnv for this purpose.
ENV JENV_PRODUCT_VERSION=0.5.5
ARG JENV_URL="https://github.com/jenv/jenv/archive/${JENV_PRODUCT_VERSION}.tar.gz"
ARG JENV_SHA512="2e312cfb4c0b2033fdc0ef4f3babfcdff2f58151247b3524589a70a0df3613d0af6abec1d044628ba321c1c0df5c5ba19d7a7bc93f41adbf4d5c0f29a3dcf9f8"
RUN mkdir -p /opt/jenv \
  && curl -fsSL -o /tmp/jenv.tar.gz "${JENV_URL}" \
  && echo "${JENV_SHA512}  /tmp/jenv.tar.gz" | sha512sum -c - \
  && tar -zxf /tmp/jenv.tar.gz -C /opt/jenv --strip-components=1 \
  && rm -f /tmp/jenv.tar.gz \
  && ln -s /opt/jenv/bin/jenv /usr/bin/jenv

# Install maven
ENV MAVEN_VERSION=3.8.6
ARG MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ARG MAVEN_SHA512="f790857f3b1f90ae8d16281f902c689e4f136ebe584aba45e4b1fa66c80cba826d3e0e52fdd04ed44b4c66f6d3fe3584a057c26dfcac544a60b301e6d0f91c26"
RUN mkdir -p /opt/maven \
  && curl -fsSL -o /tmp/apache-maven.tar.gz "${MAVEN_URL}" \
  && echo "${MAVEN_SHA512}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /opt/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /opt/maven/bin/mvn /usr/bin/mvn

# Install Apache Yetus
ENV YETUS_VERSION 0.14.1
ARG YETUS_URL="https://archive.apache.org/dist/yetus/${YETUS_VERSION}/apache-yetus-${YETUS_VERSION}-bin.tar.gz"
ARG YETUS_SHA512="068c8426a5c7ecc9281b988123d223a3d772fd725365303bde8287ae1515e623764b89f09a4a9cdc01e7f30631814086b71408b625025a51bdb3c58b213b4109"
RUN mkdir -p /opt/yetus \
  && curl -fsSL -o /tmp/apache-yetus.tar.gz "${YETUS_URL}" \
  && echo "${YETUS_SHA512} */tmp/apache-yetus.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-yetus.tar.gz -C /opt/yetus --strip-components=1 \
  && rm -f /tmp/apache-yetus.tar.gz
ENV YETUS_HOME /opt/yetus

ARG UID
ARG RM_USER
RUN groupadd hbase-rm \
  && useradd --create-home --no-log-init --shell /bin/bash --password hbase-rm --uid $UID $RM_USER \
  && mkdir /home/$RM_USER/.gnupg \
  && chown -R $RM_USER:hbase-rm /home/$RM_USER \
  && chmod -R 700 /home/$RM_USER

USER $RM_USER:hbase-rm
RUN mkdir -p "${HOME}/.jenv/versions" \
  && bash -c 'echo eval "$(jenv init -)" >> ${HOME}/.bashrc' \
  && { update-java-alternatives --list || true ; } | cut -d/ -f2- | xargs -I{} jenv add /{}
ENV PATH="/home/${RM_USER}/.jenv/shims:${PATH}"

WORKDIR /home/$RM_USER/hbase-rm/

ENTRYPOINT [ "./do-release.sh" ]

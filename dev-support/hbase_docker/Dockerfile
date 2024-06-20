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

FROM ubuntu:22.04 AS base_image
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN DEBIAN_FRONTEND=noninteractive apt-get -qq update && \
  DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends -y \
    ca-certificates=20211016 \
    curl='7.81.0-*' \
    git='1:2.34.1-*' \
    locales='2.35-*' \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \
    && \
    locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

FROM base_image AS maven_download_image
ENV MAVEN_VERSION='3.8.6'
ENV MAVEN_URL "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ENV MAVEN_SHA512 'f790857f3b1f90ae8d16281f902c689e4f136ebe584aba45e4b1fa66c80cba826d3e0e52fdd04ed44b4c66f6d3fe3584a057c26dfcac544a60b301e6d0f91c26'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/maven.tar.gz "${MAVEN_URL}" && \
  echo "${MAVEN_SHA512} */tmp/maven.tar.gz" | sha512sum -c -

FROM base_image AS openjdk8_download_image
ENV OPENJDK8_URL 'https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u352-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u352b08.tar.gz'
ENV OPENJDK8_SHA256 '1633bd7590cb1cd72f5a1378ae8294451028b274d798e2a4ac672059a2f00fee'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/adoptopenjdk8.tar.gz "${OPENJDK8_URL}" && \
  echo "${OPENJDK8_SHA256} */tmp/adoptopenjdk8.tar.gz" | sha256sum -c -

FROM base_image
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

#
# when updating java or maven versions here, consider also updating
# `dev-support/docker/Dockerfile` as well.
#

# hadolint ignore=DL3010
COPY --from=maven_download_image /tmp/maven.tar.gz /tmp/maven.tar.gz
RUN tar xzf /tmp/maven.tar.gz -C /opt && \
  ln -s "/opt/$(dirname "$(tar -tf /tmp/maven.tar.gz | head -n1)")" /opt/maven && \
  rm /tmp/maven.tar.gz

# hadolint ignore=DL3010
COPY --from=openjdk8_download_image /tmp/adoptopenjdk8.tar.gz /tmp/adoptopenjdk8.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk8.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename "$(tar -tf /tmp/adoptopenjdk8.tar.gz | head -n1)")" /usr/lib/jvm/java-8-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-8-adoptopenjdk /usr/lib/jvm/java-8 && \
  rm /tmp/adoptopenjdk8.tar.gz

ENV MAVEN_HOME '/opt/maven'
ENV JAVA_HOME '/usr/lib/jvm/java-8'
ENV PATH '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
ENV PATH "${JAVA_HOME}/bin:${MAVEN_HOME}/bin:${PATH}"

# Pull down HBase and build it into /root/hbase-bin.
WORKDIR /root
ARG BRANCH_OR_TAG=master
RUN git clone --depth 1 -b ${BRANCH_OR_TAG} https://github.com/apache/hbase.git \
    && \
    mvn -T1C clean install -DskipTests assembly:single -f ./hbase/pom.xml \
    && \
    mkdir -p hbase-bin \
    && \
    find /root/hbase/hbase-assembly/target -iname '*.tar.gz' -not -iname '*client*' \
    | head -n 1 \
    | xargs -I{} tar xzf {} --strip-components 1 -C /root/hbase-bin

# Set HBASE_HOME, add it to the path, and start HBase.
ENV HBASE_HOME /root/hbase-bin
ENV PATH "/root/hbase-bin/bin:${PATH}"

# for test hadolint

CMD ["/bin/bash", "-c", "start-hbase.sh; hbase shell"]

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

FROM ubuntu:18.04 AS BASE_IMAGE
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN DEBIAN_FRONTEND=noninteractive apt-get -qq update && \
  DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends -y \
    ca-certificates=20180409 \
    curl='7.58.0-*' \
    git='1:2.17.1-*' \
    locales='2.27-*' \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

FROM BASE_IMAGE AS MAVEN_DOWNLOAD_IMAGE
ENV MAVEN_VERSION='3.6.3'
ENV MAVEN_URL "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ENV MAVEN_SHA512 'c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/maven.tar.gz "${MAVEN_URL}" && \
  echo "${MAVEN_SHA512} */tmp/maven.tar.gz" | sha512sum -c -

FROM BASE_IMAGE AS OPENJDK8_DOWNLOAD_IMAGE
ENV OPENJDK8_URL 'https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u232-b09/OpenJDK8U-jdk_x64_linux_hotspot_8u232b09.tar.gz'
ENV OPENJDK8_SHA256 '7b7884f2eb2ba2d47f4c0bf3bb1a2a95b73a3a7734bd47ebf9798483a7bcc423'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/adoptopenjdk8.tar.gz "${OPENJDK8_URL}" && \
  echo "${OPENJDK8_SHA256} */tmp/adoptopenjdk8.tar.gz" | sha256sum -c -

FROM BASE_IMAGE
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

#
# when updating java or maven versions here, consider also updating
# `dev-support/docker/Dockerfile` as well.
#

# hadolint ignore=DL3010
COPY --from=MAVEN_DOWNLOAD_IMAGE /tmp/maven.tar.gz /tmp/maven.tar.gz
RUN tar xzf /tmp/maven.tar.gz -C /opt && \
  ln -s "/opt/$(dirname "$(tar -tf /tmp/maven.tar.gz | head -n1)")" /opt/maven && \
  rm /tmp/maven.tar.gz

# hadolint ignore=DL3010
COPY --from=OPENJDK8_DOWNLOAD_IMAGE /tmp/adoptopenjdk8.tar.gz /tmp/adoptopenjdk8.tar.gz
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
RUN git clone https://gitbox.apache.org/repos/asf/hbase.git -b master
RUN mvn clean install -DskipTests assembly:single -f ./hbase/pom.xml
RUN mkdir -p hbase-bin
RUN find /root/hbase/hbase-assembly/target -iname '*.tar.gz' -not -iname '*client*' \
  | head -n 1 \
  | xargs -I{} tar xzf {} --strip-components 1 -C /root/hbase-bin

# Set HBASE_HOME, add it to the path, and start HBase.
ENV HBASE_HOME /root/hbase-bin
ENV PATH "/root/hbase-bin/bin:${PATH}"

CMD ["/bin/bash", "-c", "start-hbase.sh; hbase shell"]

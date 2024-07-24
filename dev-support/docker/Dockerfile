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
# Dockerfile used as the build and test environment, amenable to Yetus.
#
# Built in multiple stages so as to avoid re-downloading large binaries when
# tweaking unrelated aspects of the image.

# start with a minimal image into which we can download remote tarballs
FROM ubuntu:22.04 AS base_image
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN DEBIAN_FRONTEND=noninteractive apt-get -qq update && \
  DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends -y \
    ca-certificates=20211016 \
    curl='7.81.0-*' \
    locales='2.35-*' \
##
# install dependencies from system packages.
# be careful not to install any system packages (i.e., findbugs) that will
# pull in the default-jre.
#
# bring the base image into conformance with the expectations imposed by
# Yetus and our personality file of what a build environment looks like.
    bash='5.1-*' \
    build-essential=12.9ubuntu3 \
    diffutils='1:3.8-*' \
    git='1:2.34.1-*' \
    rsync='3.2.3-*' \
    tar='1.34+dfsg-*' \
    wget='1.21.2-*' \
# install the dependencies required in order to enable the sundry precommit
# checks/features provided by Yetus plugins.
    bats='1.2.1-*' \
    libperl-critic-perl='1.140-*' \
    python3='3.10.6-*' \
    python3-pip='22.0.2+dfsg-*' \
    python3-setuptools='59.6.0-*' \
    ruby=1:3.0* \
    ruby-dev=1:3.0* \
    shellcheck='0.8.0-*' \
    libxml2-dev='2.9.13+dfsg-*' \
    libxml2-utils='2.9.13+dfsg-*' \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \
    && \
    python3 -mpip install --upgrade pip && \
    python3 -mpip install pylint==2.15.5 \
    && \
    gem install --no-document \
    rake:13.0.3 \
    rubocop:1.37.1 \
    ruby-lint:2.3.1 \
    && \
    locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

##
# download sundry dependencies
#

FROM base_image AS spotbugs_download_image
ENV SPOTBUGS_VERSION='4.7.3'
ENV SPOTBUGS_URL="https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/${SPOTBUGS_VERSION}/spotbugs-${SPOTBUGS_VERSION}.tgz"
ENV SPOTBUGS_SHA512='09a9fe0e5a6ec8e9d6d116c361b5c34c9d0560c0271241f02fadee911952adfcd69dc184f6de1cc4d4a8fe2c84c162689ea9a691dcae0779935eedf390fcc4ad'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/spotbugs.tgz "${SPOTBUGS_URL}" && \
  echo "${SPOTBUGS_SHA512} */tmp/spotbugs.tgz" | sha512sum -c -

FROM base_image AS hadolint_download_image
ENV HADOLINT_VERSION='2.10.0'
ENV HADOLINT_URL="https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-Linux-x86_64"
ENV HADOLINT_SHA512='4816c95243bedf15476d2225f487fc17465495fb2031e1a4797d82a26db83a1edb63e4fed084b80cef17d5eb67eb45508caadaf7cd0252fb061187113991a338'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/hadolint "${HADOLINT_URL}" && \
  echo "${HADOLINT_SHA512} */tmp/hadolint" | sha512sum -c -

FROM base_image AS maven_download_image
ENV MAVEN_VERSION='3.9.8'
ENV MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ENV MAVEN_SHA512='7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/maven.tar.gz "${MAVEN_URL}" && \
  echo "${MAVEN_SHA512} */tmp/maven.tar.gz" | sha512sum -c -

FROM base_image AS openjdk8_download_image
ENV OPENJDK8_URL='https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u412-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u412b08.tar.gz'
ENV OPENJDK8_SHA256='b9884a96f78543276a6399c3eb8c2fd8a80e6b432ea50e87d3d12d495d1d2808'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/adoptopenjdk8.tar.gz "${OPENJDK8_URL}" && \
  echo "${OPENJDK8_SHA256} */tmp/adoptopenjdk8.tar.gz" | sha256sum -c -

FROM base_image AS openjdk11_download_image
ENV OPENJDK11_URL='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.23%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.23_9.tar.gz'
ENV OPENJDK11_SHA256='23e47ea7a3015be3240f21185fd902adebdcf76530757c9b482c7eb5bd3417c2'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/adoptopenjdk11.tar.gz "${OPENJDK11_URL}" && \
  echo "${OPENJDK11_SHA256} */tmp/adoptopenjdk11.tar.gz" | sha256sum -c -

FROM base_image AS openjdk17_download_image
ENV OPENJDK17_URL='https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.11%2B9/OpenJDK17U-jdk_x64_linux_hotspot_17.0.11_9.tar.gz'
ENV OPENJDK17_SHA256='aa7fb6bb342319d227a838af5c363bfa1b4a670c209372f9e6585bd79da6220c'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/adoptopenjdk17.tar.gz "${OPENJDK17_URL}" && \
  echo "${OPENJDK17_SHA256} */tmp/adoptopenjdk17.tar.gz" | sha256sum -c -

##
# build the final image
#

FROM base_image
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# hadolint ignore=DL3010
COPY --from=spotbugs_download_image /tmp/spotbugs.tgz /tmp/spotbugs.tgz
RUN tar xzf /tmp/spotbugs.tgz -C /opt && \
  ln -s "/opt/$(tar -tf /tmp/spotbugs.tgz | head -n1 | cut -d/ -f1)" /opt/spotbugs && \
  chmod -R a+x /opt/spotbugs/bin/* && \
  rm /tmp/spotbugs.tgz

COPY --from=hadolint_download_image /tmp/hadolint /tmp/hadolint
RUN mv /tmp/hadolint /usr/local/bin && \
  chmod a+x /usr/local/bin/hadolint

# hadolint ignore=DL3010
COPY --from=maven_download_image /tmp/maven.tar.gz /tmp/maven.tar.gz
RUN tar xzf /tmp/maven.tar.gz -C /opt && \
  ln -s "/opt/$(dirname "$(tar -tf /tmp/maven.tar.gz | head -n1)")" /opt/maven && \
  rm /tmp/maven.tar.gz

##
# ensure JVMs are available under `/usr/lib/jvm` and prefix each installation
# as `java-` so as to conform with Yetus's assumptions.
#
# when updating java or maven versions here, consider also updating
# `dev-support/hbase_docker/Dockerfile` as well.
#

# hadolint ignore=DL3010
COPY --from=openjdk8_download_image /tmp/adoptopenjdk8.tar.gz /tmp/adoptopenjdk8.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk8.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename "$(tar -tf /tmp/adoptopenjdk8.tar.gz | head -n1)")" /usr/lib/jvm/java-8-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-8-adoptopenjdk /usr/lib/jvm/java-8 && \
  rm /tmp/adoptopenjdk8.tar.gz

# hadolint ignore=DL3010
COPY --from=openjdk11_download_image /tmp/adoptopenjdk11.tar.gz /tmp/adoptopenjdk11.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk11.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename "$(tar -tf /tmp/adoptopenjdk11.tar.gz | head -n1)")" /usr/lib/jvm/java-11-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-11-adoptopenjdk /usr/lib/jvm/java-11 && \
  rm /tmp/adoptopenjdk11.tar.gz

# hadolint ignore=DL3010
COPY --from=openjdk17_download_image /tmp/adoptopenjdk17.tar.gz /tmp/adoptopenjdk17.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk17.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename "$(tar -tf /tmp/adoptopenjdk17.tar.gz | head -n1)")" /usr/lib/jvm/java-17-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-17-adoptopenjdk /usr/lib/jvm/java-17 && \
  rm /tmp/adoptopenjdk17.tar.gz

# configure default environment for Yetus. Yetus in dockermode seems to require
# these values to be specified here; the various --foo-path flags do not
# propigate as expected, while these are honored.
# TODO (nd): is this really true? investigate and file a ticket.
ENV SPOTBUGS_HOME='/opt/spotbugs' MAVEN_HOME='/opt/maven'

CMD ["/bin/bash"]

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

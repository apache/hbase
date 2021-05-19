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
FROM ubuntu:18.04 AS BASE_IMAGE
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN DEBIAN_FRONTEND=noninteractive apt-get -qq update && \
  DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends -y \
    ca-certificates=20180409 \
    curl='7.58.0-*' \
    locales='2.27-*' \
##
# install dependencies from system packages.
# be careful not to install any system packages (i.e., findbugs) that will
# pull in the default-jre.
#
# bring the base image into conformance with the expectations imposed by
# Yetus and our personality file of what a build environment looks like.
    bash='4.4.18-*' \
    build-essential=12.4ubuntu1 \
    diffutils='1:3.6-*' \
    git='1:2.17.1-*' \
    rsync='3.1.2-*' \
    tar='1.29b-*' \
    wget='1.19.4-*' \
# install the dependencies required in order to enable the sundry precommit
# checks/features provided by Yetus plugins.
    bats='0.4.0-*' \
    libperl-critic-perl='1.130-*' \
    python3='3.6.7-*' \
    python3-pip='9.0.1-*' \
    python3-setuptools='39.0.1-*' \
    ruby=1:2.5.1 \
    ruby-dev=1:2.5.1 \
    shellcheck='0.4.6-*' \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -mpip install --upgrade pip && \
  python3 -mpip install pylint==2.4.4

RUN gem install --no-document \
  rake:13.0.1 \
  rubocop:0.80.0 \
  ruby-lint:2.3.1

RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

##
# download sundry dependencies
#

FROM BASE_IMAGE AS SPOTBUGS_DOWNLOAD_IMAGE
ENV SPOTBUGS_VERSION '4.2.2'
ENV SPOTBUGS_URL "https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/${SPOTBUGS_VERSION}/spotbugs-${SPOTBUGS_VERSION}.tgz"
ENV SPOTBUGS_SHA256 '4967c72396e34b86b9458d0c34c5ed185770a009d357df8e63951ee2844f769f'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/spotbugs.tgz "${SPOTBUGS_URL}" && \
  echo "${SPOTBUGS_SHA256} */tmp/spotbugs.tgz" | sha256sum -c -

FROM BASE_IMAGE AS HADOLINT_DOWNLOAD_IMAGE
ENV HADOLINT_VERSION '1.17.5'
ENV HADOLINT_URL "https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-Linux-x86_64"
ENV HADOLINT_SHA256 '20dd38bc0602040f19268adc14c3d1aae11af27b463af43f3122076baf827a35'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/hadolint "${HADOLINT_URL}" && \
  echo "${HADOLINT_SHA256} */tmp/hadolint" | sha256sum -c -

FROM BASE_IMAGE AS MAVEN_DOWNLOAD_IMAGE
ENV MAVEN_VERSION='3.6.3'
ENV MAVEN_URL "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ENV MAVEN_SHA512 'c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/maven.tar.gz "${MAVEN_URL}" && \
  echo "${MAVEN_SHA512} */tmp/maven.tar.gz" | sha512sum -c -

FROM BASE_IMAGE AS OPENJDK7_DOWNLOAD_IMAGE
ENV OPENJDK7_URL 'https://cdn.azul.com/zulu/bin/zulu7.36.0.5-ca-jdk7.0.252-linux_x64.tar.gz'
ENV OPENJDK7_SHA256 'e0f34c242e6d456dac3e2c8a9eaeacfa8ea75c4dfc3e8818190bf0326e839d82'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/zuluopenjdk7.tar.gz "${OPENJDK7_URL}" && \
  echo "${OPENJDK7_SHA256} */tmp/zuluopenjdk7.tar.gz" | sha256sum -c -

FROM BASE_IMAGE AS OPENJDK8_DOWNLOAD_IMAGE
ENV OPENJDK8_URL 'https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u282-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u282b08.tar.gz'
ENV OPENJDK8_SHA256 'e6e6e0356649b9696fa5082cfcb0663d4bef159fc22d406e3a012e71fce83a5c'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/adoptopenjdk8.tar.gz "${OPENJDK8_URL}" && \
  echo "${OPENJDK8_SHA256} */tmp/adoptopenjdk8.tar.gz" | sha256sum -c -

FROM BASE_IMAGE AS OPENJDK11_DOWNLOAD_IMAGE
ENV OPENJDK11_URL 'https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.10%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.10_9.tar.gz'
ENV OPENJDK11_SHA256 'ae78aa45f84642545c01e8ef786dfd700d2226f8b12881c844d6a1f71789cb99'
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --location --fail --silent --show-error --output /tmp/adoptopenjdk11.tar.gz "${OPENJDK11_URL}" && \
  echo "${OPENJDK11_SHA256} */tmp/adoptopenjdk11.tar.gz" | sha256sum -c -

##
# build the final image
#

FROM BASE_IMAGE
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# hadolint ignore=DL3010
COPY --from=SPOTBUGS_DOWNLOAD_IMAGE /tmp/spotbugs.tgz /tmp/spotbugs.tgz
RUN tar xzf /tmp/spotbugs.tgz -C /opt && \
  ln -s "/opt/$(tar -tf /tmp/spotbugs.tgz | head -n1 | cut -d/ -f1)" /opt/spotbugs && \
  chmod -R a+x /opt/spotbugs/bin/* && \
  rm /tmp/spotbugs.tgz

COPY --from=HADOLINT_DOWNLOAD_IMAGE /tmp/hadolint /tmp/hadolint
RUN mv /tmp/hadolint /usr/local/bin && \
  chmod a+x /usr/local/bin/hadolint

# hadolint ignore=DL3010
COPY --from=MAVEN_DOWNLOAD_IMAGE /tmp/maven.tar.gz /tmp/maven.tar.gz
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
COPY --from=OPENJDK7_DOWNLOAD_IMAGE /tmp/zuluopenjdk7.tar.gz /tmp/zuluopenjdk7.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/zuluopenjdk7.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename "$(tar -tf /tmp/zuluopenjdk7.tar.gz | head -n1)")" /usr/lib/jvm/java-7-zuluopenjdk && \
  ln -s /usr/lib/jvm/java-7-zuluopenjdk /usr/lib/jvm/java-7 && \
  rm /tmp/zuluopenjdk7.tar.gz

# hadolint ignore=DL3010
COPY --from=OPENJDK8_DOWNLOAD_IMAGE /tmp/adoptopenjdk8.tar.gz /tmp/adoptopenjdk8.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk8.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename "$(tar -tf /tmp/adoptopenjdk8.tar.gz | head -n1)")" /usr/lib/jvm/java-8-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-8-adoptopenjdk /usr/lib/jvm/java-8 && \
  rm /tmp/adoptopenjdk8.tar.gz

# hadolint ignore=DL3010
COPY --from=OPENJDK11_DOWNLOAD_IMAGE /tmp/adoptopenjdk11.tar.gz /tmp/adoptopenjdk11.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk11.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename "$(tar -tf /tmp/adoptopenjdk11.tar.gz | head -n1)")" /usr/lib/jvm/java-11-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-11-adoptopenjdk /usr/lib/jvm/java-11 && \
  rm /tmp/adoptopenjdk11.tar.gz

# configure default environment for Yetus. Yetus in dockermode seems to require
# these values to be specified here; the various --foo-path flags do not
# propigate as expected, while these are honored.
# TODO (nd): is this really true? investigate and file a ticket.
ENV SPOTBUGS_HOME '/opt/spotbugs'
ENV MAVEN_HOME '/opt/maven'
ENV MAVEN_OPTS '-Xmx3.6G'

CMD ["/bin/bash"]

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

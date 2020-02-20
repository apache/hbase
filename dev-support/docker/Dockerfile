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
# Dockerfile used as the build and test environment for Yetus.
#
# Built in multiple stages so as to avoid re-downloading large binaries when
# tweaking unrelated aspects of the image.

# start with a minimal image into which we can download remote tarballs
FROM ubuntu:18.04 AS BASE_IMAGE
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

RUN DEBIAN_FRONTEND=noninteractive apt-get -qq update && \
  DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends -y \
    ca-certificates \
    curl

##
# download sundry dependencies
#

FROM BASE_IMAGE AS FINDBUGS_DOWNLOAD_IMAGE
ENV FINDBUGS_VERSION '3.0.1'
ENV FINDBUGS_URL "https://downloads.sourceforge.net/project/findbugs/findbugs/${FINDBUGS_VERSION}/findbugs-${FINDBUGS_VERSION}.tar.gz"
ENV FINDBUGS_SHA256 'e80e0da0c213a27504ef3188ef25f107651700ffc66433eac6a7454bbe336419'
RUN curl -LfsSo /tmp/findbugs.tar.gz "${FINDBUGS_URL}" && \
  echo "${FINDBUGS_SHA256} */tmp/findbugs.tar.gz" | sha256sum -c -

FROM BASE_IMAGE AS HADOLINT_DOWNLOAD_IMAGE
ENV HADOLINT_VERSION '1.17.5'
ENV HADOLINT_URL "https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-Linux-x86_64"
ENV HADOLINT_SHA256 '20dd38bc0602040f19268adc14c3d1aae11af27b463af43f3122076baf827a35'
RUN curl -LfsSo /tmp/hadolint "${HADOLINT_URL}" && \
  echo "${HADOLINT_SHA256} */tmp/hadolint" | sha256sum -c -

FROM BASE_IMAGE AS MAVEN_DOWNLOAD_IMAGE
ENV MAVEN_VERSION='3.5.4'
ENV MAVEN_URL "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ENV MAVEN_SHA256 'ce50b1c91364cb77efe3776f756a6d92b76d9038b0a0782f7d53acf1e997a14d'
RUN curl -LfsSo /tmp/maven.tar.gz "${MAVEN_URL}" && \
  echo "${MAVEN_SHA256} */tmp/maven.tar.gz" | sha256sum -c -

FROM BASE_IMAGE AS ZULU7_DOWNLOAD_IMAGE
ENV ZULU7_URL 'https://cdn.azul.com/zulu/bin/zulu7.36.0.5-ca-jdk7.0.252-linux_x64.tar.gz'
ENV ZULU7_SHA256 'e0f34c242e6d456dac3e2c8a9eaeacfa8ea75c4dfc3e8818190bf0326e839d82'
RUN curl -LfsSo /tmp/zulujdk7.tar.gz "${ZULU7_URL}" && \
  echo "${ZULU7_SHA256} */tmp/zulujdk7.tar.gz" | sha256sum -c -

FROM BASE_IMAGE AS OPENJDK8_DOWNLOAD_IMAGE
ENV OPENJDK8_URL 'https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u232-b09/OpenJDK8U-jdk_x64_linux_hotspot_8u232b09.tar.gz'
ENV OPENJDK8_SHA256 '7b7884f2eb2ba2d47f4c0bf3bb1a2a95b73a3a7734bd47ebf9798483a7bcc423'
RUN curl -LfsSo /tmp/adoptopenjdk8.tar.gz "${OPENJDK8_URL}" && \
  echo "${OPENJDK8_SHA256} */tmp/adoptopenjdk8.tar.gz" | sha256sum -c -

FROM BASE_IMAGE AS OPENJDK11_DOWNLOAD_IMAGE
ENV OPENJDK11_URL 'https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jdk_x64_linux_hotspot_11.0.6_10.tar.gz'
ENV OPENJDK11_SHA256 '330d19a2eaa07ed02757d7a785a77bab49f5ee710ea03b4ee2fa220ddd0feffc'
RUN curl -LfsSo /tmp/adoptopenjdk11.tar.gz "${OPENJDK11_URL}" && \
  echo "${OPENJDK11_SHA256} */tmp/adoptopenjdk11.tar.gz" | sha256sum -c -

##
# build the final image
#

FROM BASE_IMAGE

# install dependencies from system packages. the first set brings the Ubuntu
# distro into conformance with Yetus's expectations of what a Unix looks like.
# the second set are dependencies required to enable various precommit
# checks/features.

RUN DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends -y \
  bash \
  build-essential \
  curl \
  diffutils \
  git \
  rsync \
  tar

# be careful not to install any system packages (i.e., findbugs) that will
# pull in the default-jre.
RUN DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends -y \
  bats \
  libperl-critic-perl \
  python3 \
  python3-pip \
  python3-setuptools \
  ruby \
  ruby-dev \
  shellcheck \
  && \
  rm -rf /var/lib/apt/lists/*

RUN python3 -mpip install --upgrade pip && \
  python3 -mpip install pylint

RUN gem install --no-document \
  rake \
  rubocop \
  ruby-lint

COPY --from=FINDBUGS_DOWNLOAD_IMAGE /tmp/findbugs.tar.gz /tmp/findbugs.tar.gz
RUN tar xzf /tmp/findbugs.tar.gz -C /opt && \
  ln -s "/opt/$(dirname $(tar -tf /tmp/findbugs.tar.gz | head -n1))" /opt/findbugs && \
  rm /tmp/findbugs.tar.gz

COPY --from=HADOLINT_DOWNLOAD_IMAGE /tmp/hadolint /tmp/hadolint
RUN mv /tmp/hadolint /usr/local/bin && \
  chmod a+x /usr/local/bin/hadolint

COPY --from=MAVEN_DOWNLOAD_IMAGE /tmp/maven.tar.gz /tmp/maven.tar.gz
RUN tar xzf /tmp/maven.tar.gz -C /opt && \
  ln -s "/opt/$(dirname $(tar -tf /tmp/maven.tar.gz | head -n1))" /opt/maven && \
  rm /tmp/maven.tar.gz

##
# ensure JVMs are available under `/usr/lib/jvm` and prefix each installation
# as `java-` so as to conform with Yetus's assumptions.
#

COPY --from=ZULU7_DOWNLOAD_IMAGE /tmp/zulujdk7.tar.gz /tmp/zulujdk7.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/zulujdk7.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename $(tar -tf /tmp/zulujdk7.tar.gz | head -n1))" /usr/lib/jvm/java-7-zulu && \
  ln -s /usr/lib/jvm/java-7-zulu /usr/lib/jvm/java-7 && \
  rm /tmp/zulujdk7.tar.gz

COPY --from=OPENJDK8_DOWNLOAD_IMAGE /tmp/adoptopenjdk8.tar.gz /tmp/adoptopenjdk8.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk8.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename $(tar -tf /tmp/adoptopenjdk8.tar.gz | head -n1))" /usr/lib/jvm/java-8-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-8-adoptopenjdk /usr/lib/jvm/java-8 && \
  rm /tmp/adoptopenjdk8.tar.gz

COPY --from=OPENJDK11_DOWNLOAD_IMAGE /tmp/adoptopenjdk11.tar.gz /tmp/adoptopenjdk11.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk11.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename $(tar -tf /tmp/adoptopenjdk11.tar.gz | head -n1))" /usr/lib/jvm/java-11-adoptopenjdk && \
  ln -s /usr/lib/jvm/java-11-adoptopenjdk /usr/lib/jvm/java-11 && \
  rm /tmp/adoptopenjdk11.tar.gz

# configure default environment for Yetus. Yetus in dockermode seems to require
# these values to be specified here; the various --foo-path flags do not
# propigate as expected, while these are honored.
ENV FINDBUGS_HOME '/opt/findbugs'
ENV MAVEN_HOME '/opt/maven'
ENV MAVEN_OPTS '-Xms4G -Xmx4G'

CMD /bin/bash

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

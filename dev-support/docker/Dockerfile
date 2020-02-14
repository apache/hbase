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

# start with a minimal image into which we can install remote tarballs
FROM alpine:3.11 AS INSTALL_BASE
RUN apk --update add \
  curl \
  && \
  rm -rf /var/lib/apt/lists/* && \
  rm /var/cache/apk/*

#
# download sundry dependencies
#

FROM INSTALL_BASE AS FINDBUGS_DOWNLOAD_IMAGE
ENV FINDBUGS_VERSION '3.0.1'
ENV FINDBUGS_SHA256 'e80e0da0c213a27504ef3188ef25f107651700ffc66433eac6a7454bbe336419'
RUN curl -LfsSo /tmp/findbugs.tar.gz \
    "https://downloads.sourceforge.net/project/findbugs/findbugs/${FINDBUGS_VERSION}/findbugs-${FINDBUGS_VERSION}.tar.gz" && \
  echo "${FINDBUGS_SHA256} */tmp/findbugs.tar.gz" | sha256sum -c -

FROM INSTALL_BASE AS HADOLINT_DOWNLOAD_IMAGE
ENV HADOLINT_VERSION '1.17.5'
ENV HADOLINT_SHA256 '20dd38bc0602040f19268adc14c3d1aae11af27b463af43f3122076baf827a35'
RUN curl -LfsSo /tmp/hadolint \
    "https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-Linux-x86_64" && \
  echo "${HADOLINT_SHA256} */tmp/hadolint" | sha256sum -c -

FROM INSTALL_BASE AS MAVEN_DOWNLOAD_IMAGE
ENV MAVEN_VERSION '3.5.4'
ENV MAVEN_SHA256 'ce50b1c91364cb77efe3776f756a6d92b76d9038b0a0782f7d53acf1e997a14d'
RUN curl -LfsSo /tmp/maven.tar.gz \
    "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" && \
  echo "${MAVEN_SHA256} */tmp/maven.tar.gz" | sha256sum -c -

FROM INSTALL_BASE AS ZULU7_DOWNLOAD_IMAGE
ENV ZULU7_SHA256 'e0f34c242e6d456dac3e2c8a9eaeacfa8ea75c4dfc3e8818190bf0326e839d82'
RUN curl -LfsSo /tmp/zulujdk7.tar.gz \
    'https://cdn.azul.com/zulu/bin/zulu7.36.0.5-ca-jdk7.0.252-linux_x64.tar.gz' && \
  echo "${ZULU7_SHA256} */tmp/zulujdk7.tar.gz" | sha256sum -c -

FROM INSTALL_BASE AS OPENJDK8_DOWNLOAD_IMAGE
ENV OPENJDK8_SHA256 '7b7884f2eb2ba2d47f4c0bf3bb1a2a95b73a3a7734bd47ebf9798483a7bcc423'
RUN curl -LfsSo /tmp/adoptopenjdk8.tar.gz \
    'https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u232-b09/OpenJDK8U-jdk_x64_linux_hotspot_8u232b09.tar.gz' && \
  echo "${OPENJDK8_SHA256} */tmp/adoptopenjdk8.tar.gz" | sha256sum -c -

#
# build the final image
#
FROM adoptopenjdk/openjdk11:x86_64-alpine-jdk-11.0.6_10
RUN apk --update add \
  bash \
  bats \
  build-base \
  curl \
  git \
  python3 \
  rsync \
  ruby \
  ruby-dev \
  shadow \
  shellcheck \
  tar \
  && \
  rm -rf /var/lib/apt/lists/* && \
  rm /var/cache/apk/*

RUN pip3 install --upgrade pip && \
  pip3 install pylint

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

#
# ensure JVMs are available under `/usr/lib/jvm` and prefix each installation
# as `java-` so as to conform with Yetus's assumptions.
#

COPY --from=ZULU7_DOWNLOAD_IMAGE /tmp/zulujdk7.tar.gz /tmp/zulujdk7.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/zulujdk7.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename $(tar -tf /tmp/zulujdk7.tar.gz | head -n1))" /usr/lib/jvm/java-7-zulu && \
  rm /tmp/zulujdk7.tar.gz

COPY --from=OPENJDK8_DOWNLOAD_IMAGE /tmp/adoptopenjdk8.tar.gz /tmp/adoptopenjdk8.tar.gz
RUN mkdir -p /usr/lib/jvm && \
  tar xzf /tmp/adoptopenjdk8.tar.gz -C /usr/lib/jvm && \
  ln -s "/usr/lib/jvm/$(basename $(tar -tf /tmp/adoptopenjdk8.tar.gz | head -n1))" /usr/lib/jvm/java-8-adoptopenjdk && \
  rm /tmp/adoptopenjdk8.tar.gz

# use the openjdk11 installed in the base image
RUN ln -s /opt/java/openjdk /usr/lib/jvm/java-11-adoptopenjdk

# configure default environment for Yetus
RUN ln -s /usr/lib/jvm/java-8-adoptopenjdk /usr/lib/jvm/default-jvm
ENV JAVA_HOME '/usr/lib/jvm/default-jvm'

CMD /bin/bash

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

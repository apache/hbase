
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


FROM ubuntu:trusty

WORKDIR /root

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true

######
# Install common dependencies from packages
#
# WARNING: DO NOT PUT JAVA APPS HERE! Otherwise they will install default
# Ubuntu Java.  See Java section below!
######
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
    build-essential \
    bzip2 \
    cmake \
    curl \
    doxygen \
    fuse \
    g++ \
    gcc \
    git \
    gnupg-agent \
    make \
    libbz2-dev \
    libcurl4-openssl-dev \
    libfuse-dev \
    libperl-critic-perl \
    libprotobuf-dev \
    libprotoc-dev \
    libsnappy-dev \
    libssl-dev \
    libtool \
    pinentry-curses \
    pkg-config \
    protobuf-compiler \
    protobuf-c-compiler \
    python \
    python2.7 \
    python-pip \
    rsync \
    snappy \
    zlib1g-dev

#######
# Oracle Java
#######

RUN echo "dot_style = mega" > "/root/.wgetrc"
RUN echo "quiet = on" >> "/root/.wgetrc"

RUN apt-get -q update && apt-get -q install --no-install-recommends -y software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java

# Auto-accept the Oracle JDK license
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get -q update && apt-get -q install --no-install-recommends -y oracle-java8-installer

####
# Apps that require Java
###
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
    ant \
    maven

# Fixing the Apache commons / Maven dependency problem under Ubuntu:
# See http://wiki.apache.org/commons/VfsProblems
RUN cd /usr/share/maven/lib && ln -s ../../java/commons-lang3-3.6.jar .

######
# Install findbugs
######
RUN mkdir -p /opt/findbugs && \
    curl -L -s -S \
         https://sourceforge.net/projects/findbugs/files/findbugs/3.0.1/findbugs-noUpdateChecks-3.0.1.tar.gz/download \
         -o /opt/findbugs.tar.gz && \
    tar xzf /opt/findbugs.tar.gz --strip-components 1 -C /opt/findbugs
ENV FINDBUGS_HOME /opt/findbugs

####
# Install shellcheck
####
RUN apt-get -q install -y cabal-install
RUN mkdir /root/.cabal
RUN echo "remote-repo: hackage.fpcomplete.com:http://hackage.fpcomplete.com/" >> /root/.cabal/config
#RUN echo "remote-repo: hackage.haskell.org:http://hackage.haskell.org/" > /root/.cabal/config
RUN echo "remote-repo-cache: /root/.cabal/packages" >> /root/.cabal/config
RUN cabal update
RUN cabal install shellcheck --global

####
# Install bats
####
RUN add-apt-repository -y ppa:duggan/bats
RUN apt-get -q update
RUN apt-get -q install --no-install-recommends -y bats

####
# Install pylint
####
RUN pip install pylint

####
# Install dateutil.parser
####
RUN pip install python-dateutil

####
# Install Ruby 2, based on Yetus 0.4.0 dockerfile
###
RUN echo 'gem: --no-rdoc --no-ri' >> /root/.gemrc
RUN apt-get -q install -y ruby2.0
#
# on trusty, the above installs ruby2.0 and ruby (1.9.3) exes
# but update-alternatives is broken, so we need to do some work
# to make 2.0 actually the default without the system flipping out
#
# See https://bugs.launchpad.net/ubuntu/+source/ruby2.0/+bug/1310292
#
RUN dpkg-divert --add --rename --divert /usr/bin/ruby.divert /usr/bin/ruby
RUN dpkg-divert --add --rename --divert /usr/bin/gem.divert /usr/bin/gemrc
RUN update-alternatives --install /usr/bin/ruby ruby /usr/bin/ruby2.0 1
RUN update-alternatives --install /usr/bin/gem gem /usr/bin/gem2.0 1


####
# Install rubocop
###
RUN gem install rubocop

####
# Install ruby-lint
###
RUN gem install ruby-lint

###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS -Xmx3g

###
# Everything past this point is either not needed for testing or breaks Yetus.
# So tell Yetus not to read the rest of the file:
# YETUS CUT HERE
###

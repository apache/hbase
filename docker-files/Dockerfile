##
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

FROM ubuntu:16.04

ARG CC=/usr/bin/gcc-5
ARG CXX=/usr/bin/g++-5
ARG CFLAGS="-fPIC -g -fno-omit-frame-pointer -O2 -pthread"
ARG CXXFLAGS="-fPIC -g -fno-omit-frame-pointer -O2 -pthread"

ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"

RUN apt-get update && \
    apt-get install -y vim maven inetutils-ping python-pip doxygen graphviz clang-format valgrind \
        wget libgflags-dev libgoogle-glog-dev dh-autoreconf pkg-config libssl-dev build-essential \
        libboost-all-dev libevent-dev libdouble-conversion-dev cmake libkrb5-dev git openjdk-8-jdk curl unzip && \
    pip install yapf && \
    apt-get -qq clean && \
    apt-get -y -qq autoremove && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    rm -rf /tmp/*

RUN wget https://www.cyrusimap.org/releases/cyrus-sasl-2.1.26.tar.gz ; \
    tar zxf cyrus-sasl-2.1.26.tar.gz ; \
    cd cyrus-sasl-2.1.26 ; \
    ./configure ; \
    make -j4; \
    make install ;\
    cp /usr/local/lib/sasl2/* /usr/lib/sasl2/

RUN apt-get update && \
    apt-get install -y debconf-utils && \
    echo "krb5-config krb5-config/kerberos_servers string localhost" | debconf-set-selections ; \
    echo "krb5-config krb5-config/admin_server string localhost" | debconf-set-selections ; \
    echo "krb5-config krb5-config/add_servers_realm string EXAMPLE.COM" | debconf-set-selections ; \
    echo "krb5-config krb5-config/default_realm string EXAMPLE.COM" | debconf-set-selections ; \
    apt-get install -y krb5-kdc krb5-admin-server ; \
    echo "admin" > /tmp/krb-realm.pass ; \
    echo "admin" >> /tmp/krb-realm.pass ; \
    krb5_newrealm < /tmp/krb-realm.pass ; \
    echo "addprinc hbase" > /tmp/krb-princ.pass ; \
    echo "admin" >> /tmp/krb-princ.pass ; \
    echo "admin" >> /tmp/krb-princ.pass ; \
    kadmin.local < /tmp/krb-princ.pass ; \
    echo 'addprinc hbase/securecluster' > /tmp/krb-princ.pass; echo 'admin' >> /tmp/krb-princ.pass ; \
    rm hbase-host.keytab ; echo 'admin' >> /tmp/krb-princ.pass ; \
    echo 'xst -k hbase-host.keytab hbase/securecluster@EXAMPLE.COM' >> /tmp/krb-princ.pass ; \
    kadmin.local < /tmp/krb-princ.pass ;
COPY docker-files/krb5.conf /etc

RUN git clone https://github.com/google/protobuf.git /usr/src/protobuf && \
  cd /usr/src/protobuf/ && \
  git checkout 3.5.1.1 && \
  mkdir gmock && \
  ldconfig && \
  ./autogen.sh && \
  ./configure && \
  make -j4 && \
  make install && \ 
  make clean && \
  rm -rf .git* && \
  cd /usr/src && \
  wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz && \
  tar zxf zookeeper-3.4.8.tar.gz && \ 
  rm -rf zookeeper-3.4.8.tar.gz && \
  cd zookeeper-3.4.8 && \
  cd src/c && \
  ldconfig && \
  ./configure && \
  make -j4 && \
  make install && \
  make clean && \
  ldconfig

# Update folly. `make check` removed due to a failing test, explicitly installing gtest ourselves
RUN cd /usr/src/ && \
  ver=2017.09.04.00 && \
  wget https://github.com/facebook/folly/archive/v$ver.tar.gz -O folly-$ver.tar.gz && \
  tar zxf folly-$ver.tar.gz && \
  rm -rf folly-$ver.tar.gz && \
  cd folly-$ver/folly/test && \
  wget https://github.com/google/googletest/archive/release-1.8.0.tar.gz && \
  tar zxf release-1.8.0.tar.gz && \
  rm -f release-1.8.0.tar.gz && \
  mv googletest-release-1.8.0 gtest && \
  cd gtest && \
  cmake . && \
  make -j4 && \
  make install && \
  cd ../.. && \
  ldconfig && \
  autoreconf -ivf && \
  ./configure && \
  make -j4 && \
  make install && \
  make clean

# Update wangle
RUN cd /usr/src/ && \
  ver=2017.09.04.00 && \
  wget https://github.com/facebook/wangle/archive/v$ver.tar.gz -O wangle-$ver.tar.gz && \
  tar zxf wangle-$ver.tar.gz && \
  rm -rf wangle-$ver.tar.gz && \
  cd wangle-$ver/wangle && \
  ldconfig && \
  cmake . -DBUILD_TESTS=OFF  && \
  make -j4 && \
  ctest && \
  make install && \
  make clean

RUN echo "enabled=1" >> /etc/default/apport

ENTRYPOINT /usr/sbin/krb5kdc -P /var/run/krb5kdc.pid && echo "/tmp/core.%h.%e.%t" >> /proc/sys/kernel/core_pattern && sysctl -p && ulimit -c unlimited && /bin/bash

WORKDIR /usr/src/hbase/hbase-native-client

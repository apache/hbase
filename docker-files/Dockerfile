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

FROM pjameson/buck-folly-watchman:20160511

ARG CC=/usr/bin/gcc-5
ARG CXX=/usr/bin/g++-5
ARG CFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0 -fPIC -g -fno-omit-frame-pointer -O2 -pthread"
ARG CXXFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0 -fPIC -g -fno-omit-frame-pointer -O2 -pthread"

ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"

RUN wget ftp://ftp.cyrusimap.org/cyrus-sasl/cyrus-sasl-2.1.26.tar.gz ; \
    tar zxf cyrus-sasl-2.1.26.tar.gz ; \
    cd cyrus-sasl-2.1.26 ; \
    ./configure ; \
    make ; \
    make install ;\
    cp /usr/local/lib/sasl2/* /usr/lib/sasl2/

RUN apt-get install -y vim maven inetutils-ping python-pip doxygen graphviz clang-format \
        valgrind && \
      pip install yapf && \
      apt-get -qq clean && \
      apt-get -y -qq autoremove && \
      rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
      rm -rf /tmp/*

RUN apt-get update && \
    apt-get install -y debconf-utils debconf-set-selections && \
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
  git checkout 2.7.0 && \
  mkdir gmock && \
  ldconfig && \
  ./autogen.sh && \
  ./configure && \
  make && \
  make install && \ 
  make clean && \
  rm -rf .git && \
  cd /usr/src && \
  wget http://www-us.apache.org/dist/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz && \ 
  tar zxf zookeeper-3.4.8.tar.gz && \ 
  rm -rf zookeeper-3.4.8.tar.gz && \
  cd zookeeper-3.4.8 && \
  cd src/c && \
  ldconfig && \
  ./configure && \
  make && \
  make install && \
  make clean && \
  ldconfig

ENTRYPOINT /usr/sbin/krb5kdc -P /var/run/krb5kdc.pid && /bin/bash

WORKDIR /usr/src/hbase/hbase-native-client

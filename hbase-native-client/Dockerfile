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

FROM pjameson/buck-folly-watchman

ARG CC=/usr/bin/gcc-5
ARG CXX=/usr/bin/g++-5
ARG CFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0 -fPIC -g -fno-omit-frame-pointer -O3 -pthread"
ARG CXXFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0 -fPIC -g -fno-omit-frame-pointer -O3 -pthread"

RUN apt-get install -y clang-format-3.7 vim maven inetutils-ping
RUN git clone --depth 1 --branch v2.6.1 https://github.com/google/protobuf.git /usr/src/protobuf && \
  cd /usr/src/protobuf/ && \
  ldconfig && \
  ./autogen.sh && \
  ./configure && \
  make && \
  make install && \ 
  make clean && \
  rm -rf .git

RUN cd /usr/src && \
  wget http://www-us.apache.org/dist/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz && \ 
  tar zxf zookeeper-3.4.8.tar.gz && \ 
  rm -rf zookeeper-3.4.8.tar.gz && \
  cd zookeeper-3.4.8 && \
  cd src/c && \
  ldconfig && \
  ./configure && \
  make && \
  make install && \
  make clean

RUN ldconfig

WORKDIR /usr/src/hbase/hbase-native-client

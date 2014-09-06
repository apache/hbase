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

FROM ubuntu:14.04

# Install Git, which is missing from the Ubuntu base images.
RUN apt-get update && apt-get install -y git

# Add the dependencies from the hbase_docker folder and delete ones we don't need.
WORKDIR /root
ADD . /root
RUN find . -not -name "*tar.gz" -delete

# Install Java.
RUN mkdir -p /usr/java
RUN tar xzf *jdk* --strip-components 1 -C /usr/java
ENV JAVA_HOME /usr/java

# Install Maven.
RUN mkdir -p /usr/local/apache-maven
RUN tar xzf *maven* --strip-components 1 -C /usr/local/apache-maven
ENV MAVEN_HOME /usr/local/apache-maven

# Add Java and Maven to the path.
ENV PATH /usr/java/bin:/usr/local/apache-maven/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Pull down HBase and build it into /root/hbase-bin.
RUN git clone http://git.apache.org/hbase.git -b master
RUN mvn clean install -DskipTests assembly:single -f ./hbase/pom.xml
RUN mkdir -p hbase-bin
RUN tar xzf /root/hbase/hbase-assembly/target/*tar.gz --strip-components 1 -C /root/hbase-bin

# Set HBASE_HOME, add it to the path, and start HBase.
ENV HBASE_HOME /root/hbase-bin
ENV PATH /root/hbase-bin/bin:/usr/java/bin:/usr/local/apache-maven/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

CMD ["/bin/bash", "-c", "start-hbase.sh; hbase shell"]

#!/usr/bin/env bash
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
##

hadoop_dir=$1

sed -i "s/HADOOP_TOOLS_DIR=\${HADOOP_TOOLS_DIR:-\"share\/hadoop\/tools\"}/HADOOP_TOOLS_DIR=\${HADOOP_TOOLS_DIR:-\"\$HADOOP_TOOLS_HOME\/share\/hadoop\/tools\"}/g" $1/libexec/hadoop-functions.sh
sed -i "/hadoop_add_classpath \"\${junitjar}\"/a mockitojar=\$(echo \"\${HADOOP_TOOLS_LIB_JARS_DIR}\"\/mockito-core-[0-9]*.jar)\nhadoop_add_classpath \"\${mockitojar}\"" $1/bin/mapred
curl https://repo1.maven.org/maven2/org/mockito/mockito-core/2.28.2/mockito-core-2.28.2.jar -o $1/share/hadoop/tools/lib/mockito-core-2.28.2.jar

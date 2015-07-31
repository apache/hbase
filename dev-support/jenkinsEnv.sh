#!/usr/bin/env bash
#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.

#!/bin/bash

##To set Jenkins Environment Variables:
export JAVA_HOME=/home/jenkins/tools/java/latest
export ANT_HOME=/home/jenkins/tools/ant/latest
export XERCES_HOME=/home/jenkins/tools/xerces/latest
export ECLIPSE_HOME=/home/jenkins/tools/eclipse/latest
export FORREST_HOME=/home/jenkins/tools/forrest/latest
export JAVA5_HOME=/home/jenkins/tools/java5/latest
export FINDBUGS_HOME=/home/jenkins/tools/findbugs/latest
export CLOVER_HOME=/home/jenkins/tools/clover/latest
export MAVEN_HOME=/home/jenkins/tools/maven/latest

export PATH=$PATH:$JAVA_HOME/bin:$ANT_HOME/bin:
export MAVEN_OPTS="-Xmx3100M -XX:-UsePerfData"

ulimit -n


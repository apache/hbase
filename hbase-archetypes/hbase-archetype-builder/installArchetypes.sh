#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e      # exit if any step of this script fails
workingDir=$(pwd)
buildArchetypeSubdir=target/build-archetype
archetypeSourceSubdir=target/generated-sources/archetype

# INSTALL hbase-client archetype
cd /"$workingDir"/../hbase-client-project/$buildArchetypeSubdir/$archetypeSourceSubdir
mvn install

# INSTALL hbase-shaded-client archetype
cd /"$workingDir"/../hbase-shaded-client-project/$buildArchetypeSubdir/$archetypeSourceSubdir
mvn install

# add entries for additional archetypes above this comment (modeled on entries above)

cd "$workingDir"

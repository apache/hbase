#
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

# Moves regions. Will confirm region access in current location and will
# not move a new region until successful confirm of region loading in new
# location. Presumes balancer is disabled when we run (not harmful if its
# on but this script and balancer will end up fighting each other).
$BIN=File.dirname(__FILE__)
exec "#{$BIN}/hbase org.apache.hadoop.hbase.util.RegionMover #{ARGV.join(' ')}"

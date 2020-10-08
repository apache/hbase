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

# Ruby has a stdlib named 'shell' so using "require 'shell'" does not
# work if our shell implementation is not on the local filesystem.
# this is the absolute path to our shell implementation when packaged
# in a jar. The level of indirection provided by this file lets things
# still behave the same as in earlier releases if folks unpackage the
# jar contents onto the local filesystem if they need that for some
# other reason.
require 'uri:classloader:/shell.rb'

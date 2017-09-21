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
find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h)
  # make sure we don't accidentally pick up a different version
find_library(GFLAGS_SHARED_LIB gflags)
find_library(GFLAGS_STATIC_LIB libgflags.a)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GFLAGS REQUIRED_VARS
  GFLAGS_SHARED_LIB GFLAGS_STATIC_LIB GFLAGS_INCLUDE_DIR)

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
find_path(GMOCK_INCLUDE_DIR gmock/gmock.h)
  # make sure we don't accidentally pick up a different version
find_library(GMOCK_SHARED_LIB gmock)
find_library(GMOCK_STATIC_LIB libgmock.a)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GMOCK REQUIRED_VARS
  GMOCK_SHARED_LIB GMOCK_STATIC_LIB GMOCK_INCLUDE_DIR)

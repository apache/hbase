# Licensed to the Apache Software Foundation (ASF) under one
#
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
#
find_path(FOLLY_ROOT_DIR
    NAMES include/folly/AtomicHashMap.h
)
find_library(FOLLY_LIBRARIES
    NAMES folly
    HINTS ${FOLLY_ROOT_DIR}/lib /usr/lib/ /usr/local/lib/ /usr/lib/x86_64-linux-gnu/
)
find_library(FOLLY_BENCHMARK_LIBRARIES
    NAMES follybenchmark
    HINTS ${FOLLY_ROOT_DIR}/lib
)
find_path(FOLLY_INCLUDE_DIR
    NAMES folly/AtomicHashMap.h
    HINTS ${FOLLY_ROOT_DIR}/include
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Folly DEFAULT_MSG
    FOLLY_LIBRARIES
    FOLLY_INCLUDE_DIR
)
mark_as_advanced(
    FOLLY_ROOT_DIR
    FOLLY_LIBRARIES
    FOLLY_BENCHMARK_LIBRARIES
    FOLLY_INCLUDE_DIR
)
if (FOLLY_LIBRARIES)
  set(FOLLY_FOUND "true")
  message("-- Folly found, ${FOLLY_LIBRARIES}")
endif(FOLLY_LIBRARIES)

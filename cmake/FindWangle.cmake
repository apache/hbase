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
find_path(WANGLE_ROOT_DIR
    NAMES include/wangle/acceptor/Acceptor.h
)
find_library(WANGLE_LIBRARIES
    NAMES wangle
    HINTS ${WANGLE_ROOT_DIR}/lib /usr/lib/ /usr/local/lib/
)
find_path(WANGLE_INCLUDE_DIR
    NAMES wangle/acceptor/Acceptor.h
    HINTS ${WANGLE_ROOT_DIR}/include /usr/local/include/
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(WANGLE DEFAULT_MSG
    WANGLE_LIBRARIES
    WANGLE_INCLUDE_DIR
)
mark_as_advanced(
    WANGLE_ROOT_DIR
    WANGLE_LIBRARIES
    WANGLE_INCLUDE_DIR
)
if (WANGLE_LIBRARIES)
  set(WANGLE_FOUND "true")
  message("-- Wangle found, ${WANGLE_LIBRARIES}")
endif(WANGLE_LIBRARIES)

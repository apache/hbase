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

set( LIBEV_INCLUDE_SEARCH
  ${CMAKE_SOURCE_DIR}/thirdparty/installed/include
)

set( LIBEV_LIB_SEARCH
  ${CMAKE_SOURCE_DIR}/thirdparty/installed/include
)

find_path(LIBEV_INCLUDE
  NAMES ev++.h
  PATHS ${LIBEV_INCLUDE_SEARCH}
  NO_DEFAULT_PATH
)

find_library(LIBEV_LIBRARY
  NAMES ev
  PATHS ${LIBEV_LIB_SEARCH}
  NO_DEFAULT_PATH
)

if (LIBEV_INCLUDE_PATH AND LIBEV_LIBRARY)
  set(LIBEV_LIBS ${LIBEV_LIBRARY})
  set(LIBEV_INCLUDE_DIR ${LIBEV_INCLUDE})
  set(LIBEV_FOUND TRUE)
endif()

mark_as_advanced(
  LIBEV_INCLUDE_DIR
  LIBEV_LIBS
)

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

cmake_minimum_required(VERSION 2.6)

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

STRING(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

set(CXX_COMMON_FLAGS "-Wall -Wextra -std=c++11")
set(C_COMMON_FLAGS "-Wall -Wextra -std=c11")

set(CXX_FLAGS_DEBUG "-ggdb -g")
set(C_FLAGS_DEBUG "-ggdb")

set(CXX_FLAGS_FASTDEBUG "-ggdb -O1")
set(C_FLAGS_FASTDEBUG "-ggdb -O1")

set(CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG -Wno-strict-aliasing")
set(C_FLAGS_RELEASE "-O3 -g -DNDEBUG -Wno-strict-aliasing")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Set compile flags based on the build type.
message("Configured for ${CMAKE_BUILD_TYPE} build (set with cmake -DCMAKE_BUILD_TYPE={release,debug,...})")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
  set(CMAKE_C_FLAGS ${C_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_FASTDEBUG})
  set(CMAKE_C_FLAGS ${C_FLAGS_FASTDEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
  set(CMAKE_C_FLAGS ${C_FLAGS_RELEASE})
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
set(CMAKE_C_FLAGS "${C_COMMON_FLAGS} ${CMAKE_C_FLAGS}")


# set compile output directory
string(TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_LOWERCASE)
set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/${CMAKE_BUILD_TYPE_LOWERCASE}/")

# Link build/latest to the current build directory, to avoid developers
# accidentally running the latest debug build when in fact they're building
# release builds.
file(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
execute_process(COMMAND rm -f ${CMAKE_CURRENT_SOURCE_DIR}/build/latest)
execute_process(COMMAND ln -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
  ${CMAKE_CURRENT_SOURCE_DIR}/build/latest)

# where to put generated libraries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

include_directories(src)

#####################
# Third Party
#####################

# find boost headers and libs
set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED COMPONENTS thread system-mt)
include_directories(${Boost_INCLUDE_DIRS})

find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})
add_library(protobuf STATIC IMPORTED)
set_target_properties(protobuf PROPERTIES IMPORTED_LOCATION "${PROTOBUF_STATIC_LIBRARY}")

find_package(LibEv REQUIRED)
include_directories(${LIBEV_INCLUDE_DIR})
add_library(libev STATIC IMPORTED)


set(HBASE_LIBS
    ${PROTOBUF}
    ${LIBEV}
    ${Boost_LIBRARIES}
)
#########
# Testing Config
#########

# find GTest headers and libs
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION "${GTEST_LIBS}")

set(HBASE_TEST_LIBS ${HBASE_LIBS} ${GTEST_LIBS})
set(HBASE_ASYNC_TEST_LIBS ${HBASE_LIBS} ${GTEST_LIBS} hbase-async)
set(HBASE_SYNC_TEST_LIBS ${HBASE_LIBS} ${GTEST_LIBS} hbase-sync)

function(ADD_HBASE_ASYNC_TEST TEST_NAME)
    add_executable(${TEST_NAME} ${TEST_NAME}.cc)
    target_link_libraries(${TEST_NAME} ${HBASE_ASYNC_TEST_LIBS})
    add_test(${TEST_NAME} "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
endfunction()


function(ADD_HBASE_SYNC_TEST TEST_NAME)
    add_executable(${TEST_NAME} ${TEST_NAME}.cc)
    target_link_libraries(${TEST_NAME} ${HBASE_ASYNC_TEST_LIBS})
    add_test(${TEST_NAME} "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
endfunction()


#####
# Actual project definition
#####
add_subdirectory(src/async)
add_subdirectory(src/sync)

add_subdirectory(src/core)
add_subdirectory(src/rpc)


add_library(hbase-async SHARED $<TARGET_OBJECTS:hcore> $<TARGET_OBJECTS:hasync>)
add_library(hbase-sync SHARED $<TARGET_OBJECTS:hcore> $<TARGET_OBJECTS:hsync>)

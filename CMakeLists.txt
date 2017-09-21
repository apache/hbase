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
cmake_minimum_required(VERSION 3.0)
PROJECT(hbaseclient CXX)
set(PROJECT_NAME "hbaseclient")
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 1 )
set(PROJECT_VERSION_PATCH 0)
set(BUILD_SHARED_LIBS ON)
## set our cmake module path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
## include the Protobuf generation code
include(ProtobufGen)
if(COMPILER_SUPPORTS_CXX14)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++14 support. Please use a different C++ compiler.")
endif()
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(HBASE_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/hbase")
set(HBASE_PROTO_GEN_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/hbase/if")
set(HBASE_PROTO_GEN_INC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include/hbase/if")
# Set the right openssl root path
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl/")
else()
    set(OPENSSL_ROOT_DIR "/usr/lib/x86_64-linux-gnu")
endif()
# Include OpenSSL
find_package (OpenSSL REQUIRED)
if (OPENSSL_FOUND)
    include_directories(${OPENSSL_INCLUDE_DIR})
else ()
    message( FATAL_ERROR "OpenSSL was not found. Please install OpenSSL" )
endif (OPENSSL_FOUND)
# ensure we have required dependencies
find_package(Threads)
find_package(Boost REQUIRED COMPONENTS thread system filesystem)
find_package(Gflags REQUIRED)
find_package(Folly REQUIRED)
find_package(Krb5 REQUIRED)
find_package(Sasl2 REQUIRED)
find_package(Wangle REQUIRED)
find_package(GTest)
find_package(Glog)
find_package(Java REQUIRED)
find_package(JNI REQUIRED)
find_package(Zookeeper REQUIRED)
find_package(Protobuf REQUIRED)
if (NOT FOLLY_FOUND)
 message(FATAL_ERROR "-- Folly not found")
endif()
if (NOT WANGLE_FOUND)
 message(FATAL_ERROR "-- Wangle not found")
endif()
if (NOT PROTOBUF_FOUND)
	message(FATAL_ERROR "-- Protocol buffer include directory not found ${PROTOBUF_INCLUDE_DIRS}")
endif()
if (NOT JNI_FOUND)
        message(FATAL_ERROR "-- JAVA include directory not found")
endif()
### provide the include directories, starting at the base
### and including those from our
include_directories(include)
include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${Zookeeper_INCLUDE_DIRS})
include_directories(${Boost_INCLUDE_DIR})
include_directories(${KRB5_INCLUDE_DIRS})
include_directories(${JAVA_INCLUDE_DIRS})
include_directories(${FOLLY_INCLUDE_DIRS})
### create a directory for the hbase protobuf headers.
### this is helpful so that when we include it, later, we can generate
### the protocol buffer source/headers without polluting our tree.
set(CMAKE_BINARY_DIR_GEN "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_FILES_DIRECTORY}/hbase/if/")
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR_GEN})
## build a recursive list of sources
file(GLOB_RECURSE CONNECTION_SRC "${HBASE_SRC_DIR}/connection/*.cc"  )
file(GLOB_RECURSE EXCEPTION_SRC "${HBASE_SRC_DIR}/exceptions/*.cc"  )
file(GLOB_RECURSE PROTO_SRC "${HBASE_SRC_DIR}/if/*.cc"  )
file(GLOB_RECURSE UTILS_SRC "${HBASE_SRC_DIR}/utils/*.cc"  )
file(GLOB_RECURSE CLIENT_SRC "${HBASE_SRC_DIR}/client/*.cc"  )
file(GLOB_RECURSE SECURITY_SRC "${HBASE_SRC_DIR}/security/*.cc"  )
file(GLOB_RECURSE SRDE_SRC "${HBASE_SRC_DIR}/serde/*.cc"  )
file(GLOB_RECURSE TEST_UTIL "${HBASE_SRC_DIR}/test-util/*.cc"  )
include_directories(${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_FILES_DIRECTORY})
include_directories(${SASL_INCLUDE_DIRS})
include_directories(${GFLAGS_INCLUDE_DIR})
file(GLOB_RECURSE PROTO_FILES "${HBASE_SRC_DIR}/if/*.proto"  )
### generate the protocol buffers.
generate_protobuf_src(PROTO_SOURCES PROTO_HEADERS  ${PROTO_FILES})

add_library(hbaseclient-static STATIC ${PROTO_SOURCES} ${CLIENT_SRC} ${CONNECTION_SRC} ${EXCEPTION_SRC} ${PROTO_SRC} ${SECURITY_SRC} ${SRDE_SRC} ${UTILS_SRC})
set_target_properties(hbaseclient-static PROPERTIES LINKER_LANGUAGE CXX)
SET_TARGET_PROPERTIES(hbaseclient-static PROPERTIES OUTPUT_NAME hbaseclient CLEAN_DIRECT_OUTPUT 1)
target_link_libraries(hbaseclient-static ${PROTOBUF_LIBRARY})
target_link_libraries(hbaseclient-static ${FOLLY_LIBRARIES})
target_link_libraries(hbaseclient-static ${Boost_LIBRARIES})
target_link_libraries(hbaseclient-static ${SASL_LIBS})
target_link_libraries(hbaseclient-static ${GLOG_SHARED_LIB})
target_link_libraries(hbaseclient-static ${GFLAGS_SHARED_LIB})
target_link_libraries(hbaseclient-static ${KRB5_LIBRARIES})
target_link_libraries(hbaseclient-static ${WANGLE_LIBRARIES})
target_link_libraries(hbaseclient-static ${Zookeeper_LIBRARIES})
target_link_libraries(hbaseclient-static ${OPENSSL_LIBRARIES})
add_library(hbaseclient-shared SHARED ${PROTO_SOURCES} ${CLIENT_SRC} ${CONNECTION_SRC} ${EXCEPTION_SRC} ${PROTO_SRC} ${SECURITY_SRC} ${SRDE_SRC} ${UTILS_SRC})
set_target_properties(hbaseclient-shared PROPERTIES LINKER_LANGUAGE CXX)
SET_TARGET_PROPERTIES(hbaseclient-shared PROPERTIES COMPILE_FLAGS " -fPIC")
SET_TARGET_PROPERTIES(hbaseclient-shared PROPERTIES OUTPUT_NAME hbaseclient CLEAN_DIRECT_OUTPUT 1)
target_link_libraries(hbaseclient-shared ${PROTOBUF_LIBRARY})
target_link_libraries(hbaseclient-shared ${FOLLY_LIBRARIES})
target_link_libraries(hbaseclient-shared ${Boost_LIBRARIES})
target_link_libraries(hbaseclient-shared ${SASL_LIBS})
target_link_libraries(hbaseclient-shared ${GLOG_SHARED_LIB})
target_link_libraries(hbaseclient-shared ${GFLAGS_SHARED_LIB})
target_link_libraries(hbaseclient-shared ${KRB5_LIBRARIES})
target_link_libraries(hbaseclient-shared ${WANGLE_LIBRARIES})
target_link_libraries(hbaseclient-shared ${Zookeeper_LIBRARIES})
add_executable(simple-client "${HBASE_SRC_DIR}/examples/simple-client.cc")
SET_TARGET_PROPERTIES(simple-client PROPERTIES COMPILE_FLAGS "  ")
target_link_libraries(simple-client ${PROTOBUF_LIBRARY})
target_link_libraries(simple-client ${Boost_LIBRARIES})
target_link_libraries(simple-client ${SASL_LIBS})
target_link_libraries(simple-client ${GFLAGS_SHARED_LIB})
target_link_libraries(simple-client ${KRB5_LIBRARIES})
target_link_libraries(simple-client ${Zookeeper_LIBRARIES})
target_link_libraries(simple-client hbaseclient-static ${CMAKE_THREAD_LIBS_INIT})
add_executable(load-client "${HBASE_SRC_DIR}/examples/load-client.cc")
SET_TARGET_PROPERTIES(load-client PROPERTIES COMPILE_FLAGS "  ")
target_link_libraries(load-client ${PROTOBUF_LIBRARY})
target_link_libraries(load-client ${Boost_LIBRARIES})
target_link_libraries(load-client ${SASL_LIBS})
target_link_libraries(load-client ${GFLAGS_SHARED_LIB})
target_link_libraries(load-client ${KRB5_LIBRARIES})
target_link_libraries(load-client ${Zookeeper_LIBRARIES})
target_link_libraries(load-client hbaseclient-static ${CMAKE_THREAD_LIBS_INIT})
if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()
if (NOT SKIP_TESTS)
	include(BuildTests)
endif()
## Create a custom target for our linter
add_custom_target(
    linter
    COMMAND ${CMAKE_SOURCE_DIR}/bin/cpplint.sh)
# Copy the version.h file in before doing anything
add_custom_target (
	copy_version_h
        COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/bin/copy-version.sh"
)
add_dependencies(hbaseclient-static copy_version_h)
add_dependencies(hbaseclient-shared copy_version_h)

# Install library headers
include(GNUInstallDirs)
file(GLOB RECURSE HEADERS include/*.h)
set_target_properties(hbaseclient-static PROPERTIES PUBLIC_HEADER "${HEADERS}")
install(TARGETS hbaseclient-static
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION include/
    COMPONENT LIBRARY )
install(TARGETS hbaseclient-shared
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION include/
    COMPONENT LIBRARY )
INSTALL (
    DIRECTORY ${CMAKE_SOURCE_DIR}/include/
    DESTINATION include/
    FILES_MATCHING PATTERN "*.h*")
# Install pb-generated headers too
INSTALL (
    DIRECTORY "${CMAKE_BINARY_DIR_GEN}"
    DESTINATION include/hbase/if
    FILES_MATCHING PATTERN "hbase/if/*.h")

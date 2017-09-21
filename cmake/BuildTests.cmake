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
### test functions
MACRO(GETSOURCEFILES result curdir)
  FILE(GLOB children RELATIVE ${curdir} ${curdir}/*)
  SET(dirlist "")
  FOREACH(child ${children})
    IF( "${child}" MATCHES ^[^.].*\\.cc)

      LIST(APPEND dirlist ${child})
    ENDIF()
  ENDFOREACH()
  SET(${result} ${dirlist})
ENDMACRO()
find_package(GMock REQUIRED)
add_library(testutil STATIC ${TEST_UTIL})
target_include_directories(testutil PRIVATE BEFORE "include")
target_include_directories(testutil PRIVATE BEFORE "${Java_INCLUDE_DIRS}")
target_include_directories(testutil PRIVATE BEFORE "${JNI_INCLUDE_DIRS}")
target_include_directories(testutil PRIVATE BEFORE "${Boost_INCLUDE_DIR}")
target_include_directories(testutil PRIVATE BEFORE "${GTEST_INCLUDE_DIRS}")
target_include_directories(testutil PRIVATE BEFORE${PROTOBUF_INCLUDE_DIRS})
target_include_directories(testutil PRIVATE BEFORE${Zookeeper_INCLUDE_DIRS})
target_include_directories(testutil PRIVATE BEFORE${KRB5_INCLUDE_DIRS})
target_include_directories(testutil PRIVATE BEFORE${Java_INCLUDE_DIRS})
target_include_directories(testutil PRIVATE BEFORE${FOLLY_INCLUDE_DIRS})
target_link_libraries(testutil hbaseclient-static ${CMAKE_THREAD_LIBS_INIT} ${Java_LIBRARIES} ${JNI_LIBRARIES} ${PROTOBUF_LIBRARY} ${Boost_LIBRARIES} ${GFLAGS_SHARED_LIB} ${GMOCK_SHARED_LIB} ${GTEST_BOTH_LIBRARIES} ${SASL_LIBS} ${GFLAGS_SHARED_LIB} ${KRB5_LIBRARIES} ${OPENSSL_LIBRARIES} ${Zookeeper_LIBRARIES})
function(createTests testName)
   message ("-- Including Test: ${testName}")
    target_include_directories(${testName} PRIVATE BEFORE "include")
    target_include_directories(${testName} PRIVATE BEFORE "${Java_INCLUDE_DIRS}")
    target_include_directories(${testName} PRIVATE BEFORE "${JNI_INCLUDE_DIRS}")
    target_include_directories(${testName} PRIVATE BEFORE "${Boost_INCLUDE_DIR}")
    target_include_directories(${testName} PRIVATE BEFORE "${GTEST_INCLUDE_DIRS}")
    target_include_directories(${testName} PRIVATE BEFORE "${OPENSSL_INCLUDE_DIR}")

    target_link_libraries(hbaseclient-static ${PROTOBUF_LIBRARY})
    target_link_libraries(hbaseclient-static ${FOLLY_LIBRARIES})

    target_link_libraries(${testName} hbaseclient-static testutil ${CMAKE_THREAD_LIBS_INIT}
    ${Java_LIBRARIES}
    ${JNI_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${Boost_LIBRARIES}
    ${GFLAGS_SHARED_LIB}
    ${GTEST_BOTH_LIBRARIES}
    ${SASL_LIBS}
    ${GFLAGS_SHARED_LIB}
    ${KRB5_LIBRARIES}
    ${Zookeeper_LIBRARIES} ${OPENSSL_LIBRARIES}
    ${WANGLE_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${GLOG_SHARED_LIB})
endfunction()
enable_testing(test)
SET(TEST_DIR ${CMAKE_SOURCE_DIR}/src/test)
GETSOURCEFILES(UNIT_TESTS "${TEST_DIR}")
SET(UNIT_TEST_COUNT 0)
FOREACH(testfile ${UNIT_TESTS})
	get_filename_component(testfilename "${testfile}" NAME_WE)
	add_executable("${testfilename}" "${TEST_DIR}/${testfile}")
	createTests("${testfilename}")
  MATH(EXPR UNIT_TEST_COUNT "${UNIT_TEST_COUNT}+1")
	add_test(NAME "${testfilename}" COMMAND "${testfilename}" WORKING_DIRECTORY ${TEST_DIR})
ENDFOREACH()
message("-- Finished building ${UNIT_TEST_COUNT} unit test file(s)...")

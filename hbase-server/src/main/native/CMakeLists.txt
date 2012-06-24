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

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

# If JVM_ARCH_DATA_MODEL is 32, compile all binaries as 32-bit.
# This variable is set by maven.
if (JVM_ARCH_DATA_MODEL EQUAL 32)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
    set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} -m32")
    if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
        set(CMAKE_SYSTEM_PROCESSOR "i686")
    endif ()
endif (JVM_ARCH_DATA_MODEL EQUAL 32)

FIND_PACKAGE(JNI REQUIRED)
INCLUDE_DIRECTORIES(${JAVA_INCLUDE_PATH})
INCLUDE_DIRECTORIES(${JAVA_INCLUDE_PATH2})

add_library (mlockall_agent SHARED src/mlockall_agent/mlockall_agent.c)

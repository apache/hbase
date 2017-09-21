# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
if (MSVC)
    if(${CMAKE_BUILD_TYPE} MATCHES "Debug")
        set(ZK_BuildOutputDir "Debug")
    else()
        set(ZK_BuildOutputDir "Release")
    endif()
    if("${ZOOKEEPER_HOME}_" MATCHES  "^_$")
        message(" ")
        message("- Please set the cache variable ZOOKEEPER_HOME to point to the directory with the zookeeper source.")
        message("- CMAKE will look for zookeeper include files in $ZOOKEEPER_HOME/src/c/include.")
        message("- CMAKE will look for zookeeper library files in $ZOOKEEPER_HOME/src/c/Debug or $ZOOKEEPER_HOME/src/c/Release.")
    else()
        FILE(TO_CMAKE_PATH ${ZOOKEEPER_HOME} Zookeeper_HomePath)
        set(Zookeeper_LIB_PATHS ${Zookeeper_HomePath}/src/c/${ZK_BuildOutputDir})
        find_path(ZK_INCLUDE_DIR zookeeper.h ${Zookeeper_HomePath}/src/c/include)
        find_path(ZK_INCLUDE_DIR_GEN zookeeper.jute.h ${Zookeeper_HomePath}/src/c/generated)
        set(Zookeeper_INCLUDE_DIR zookeeper.h ${ZK_INCLUDE_DIR} ${ZK_INCLUDE_DIR_GEN} )
        find_library(Zookeeper_LIBRARY NAMES zookeeper PATHS ${Zookeeper_LIB_PATHS})
    endif()
else()
    set(Zookeeper_LIB_PATHS /usr/local/lib /usr/lib/ /usr/lib/x86_64-linux-gnu/)
    find_path(Zookeeper_INCLUDE_DIR zookeeper/zookeeper.h /usr/local/include)
    find_library(Zookeeper_LIBRARY NAMES libzookeeper_mt.a PATHS ${Zookeeper_LIB_PATHS})
endif()
set(Zookeeper_LIBRARIES ${Zookeeper_LIBRARY} )
set(Zookeeper_INCLUDE_DIRS ${Zookeeper_INCLUDE_DIR} )
include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set Zookeeper_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(Zookeeper  DEFAULT_MSG
    Zookeeper_LIBRARY Zookeeper_INCLUDE_DIR)
if (Zookeeper_LIBRARY)
   message("-- Zookeeper found, ${Zookeeper_LIBRARY}")
endif()
mark_as_advanced(Zookeeper_INCLUDE_DIR Zookeeper_LIBRARY )

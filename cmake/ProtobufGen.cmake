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
# copied in most part from protobuf cmake
# there are similar protobuf gen changes online, all of which do
# a similar job of customizing their generation.
function(generate_protobuf_src SRCS HDRS)
  if(NOT ARGN)
    message(SEND_ERROR "Error: generate_protobuf_src() called without any proto files")
    return()
  endif()

  set(_protobuf_include_path -I .)
  if(DEFINED PROTOBUF_INCLUDE_DIRS)
    foreach(DIR ${PROTOBUF_INCLUDE_DIRS})
      file(RELATIVE_PATH REL_PATH ${CMAKE_SOURCE_DIR} ${DIR})
      list(FIND _protobuf_include_path ${REL_PATH} _contains_already)
      if(${_contains_already} EQUAL -1)
        list(APPEND _protobuf_include_path -I ${REL_PATH})
      endif()
    endforeach()
  endif()
  set(${SRCS})
  set(${HDRS})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)
    ## get the directory where our protobufs are stored
    file(RELATIVE_PATH REL_FIL ${CMAKE_SOURCE_DIR} ${ABS_FIL})
    get_filename_component(REL_DIR ${REL_FIL} DIRECTORY)
    list(APPEND ${SRCS} "${CMAKE_BINARY_DIR_GEN}/${FIL_WE}.pb.cc")
    list(APPEND ${HDRS} "${CMAKE_BINARY_DIR_GEN}/${FIL_WE}.pb.h")
    add_custom_command(
      OUTPUT "${CMAKE_BINARY_DIR_GEN}/${FIL_WE}.pb.cc"
             "${CMAKE_BINARY_DIR_GEN}/${FIL_WE}.pb.h"
      COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --cpp_out=${CMAKE_BINARY_DIR_GEN}
	      --proto_path=${REL_DIR}
           ${_protobuf_include_path}
           ${REL_FIL}
      DEPENDS ${ABS_FIL} ${PROTOBUF_PROTOC_EXECUTABLE}
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      COMMENT "Generating ${FIL}"
      VERBATIM)
  endforeach()
  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()

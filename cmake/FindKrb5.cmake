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
find_path(KRB5_ROOT_DIR
    NAMES include/krb5/krb5.h
)
find_library(KRB5_LIBRARIES
    NAMES krb5
    HINTS ${KRB5_ROOT_DIR}/lib
)
find_path(KRB5_INCLUDE_DIR
    NAMES krb5/krb5.h
    HINTS ${KRB5_ROOT_DIR}/include
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(krb5 DEFAULT_MSG
    KRB5_LIBRARIES
    KRB5_INCLUDE_DIR
)
if (KRB5_LIBRARIES)
   set(KRB5_FOUND "true")
   message("-- KRB5 Libs Found, ${KRB5_LIBRARIES}")
endif()
mark_as_advanced(
    KRB5_ROOT_DIR
    KRB5_LIBRARIES
    KRB5_INCLUDE_DIR
)

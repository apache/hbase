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

set( ASYNC_SRC
  hbase_admin.cc
  hbase_client.cc
  hbase_connection.cc
  hbase_get.cc
  hbase_mutations.cc
  hbase_result.cc
  hbase_scanner.cc
)


add_library(hasync OBJECT ${ASYNC_SRC})

ADD_HBASE_ASYNC_TEST(mutations-test)
ADD_HBASE_ASYNC_TEST(get-test)

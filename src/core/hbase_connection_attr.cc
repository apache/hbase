/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "core/hbase_connection_attr.h"

#include "core/hbase_macros.h"
#include "core/hbase_types.h"
#include "core/connection_attr.h"

extern "C" {
int32_t hb_connection_attr_create(hb_connection_attr_t * attr_ptr) {
  (*attr_ptr) = new ConnectionAttr();
  return (attr_ptr == NULL)?-1:0;
}

int32_t hb_connection_attr_set_zk_quorum(hb_connection_t connection,
    char * zk_quorum) {
  return 0;
}

int32_t hb_connection_attr_set_zk_root(hb_connection_t connection,
    char * zk_root) {
  return 0;
}
}

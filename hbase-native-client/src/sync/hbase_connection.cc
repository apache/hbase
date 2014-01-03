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

#include "sync/hbase_connection.h"

#include "core/connection.h"
#include "core/hbase_types.h"

extern "C" {
int32_t hb_connection_create(hb_connection_t * connection_ptr,
    hb_connection_attr_t connection_attr) {
  (*connection_ptr) = reinterpret_cast<hb_connection_t>(new Connection());
  if ((*connection_ptr) == NULL)
    return -1;
  return 0;
}
int32_t hb_connection_destroy(hb_connection_t connection) {
  free(connection);
  return 0;
}
}   // extern "C"

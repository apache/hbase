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

#include "async/hbase_client.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "core/client.h"
#include "async/hbase_connection.h"

int32_t hb_client_create(hb_client_t* client_ptr,
    hb_connection_t connection) {
  (*client_ptr) = reinterpret_cast<hb_client_t>(new Client());
  if (client_ptr == NULL)
    return -1;  // TODO(eclark): setup the errno file.
  return 0;
}

int32_t hb_client_destroy(hb_client_t client,
    hb_client_disconnection_cb cb, void * extra) {
  if (client == NULL)
    return -2;
  if (cb) {
    cb(0, client, extra);
  }
  free(client);
  return 0;
}


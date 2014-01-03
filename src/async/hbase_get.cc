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

#include "async/hbase_get.h"

#include <stdlib.h>
#include <string.h>

#include "core/get.h"

int32_t hb_get_create(hb_get_t * get_ptr) {
  (*get_ptr) = reinterpret_cast<hb_get_t>(new Get());
  if ((*get_ptr) == NULL) {
    return -1;
  }
  return 0;
}

int32_t hb_get_destroy(hb_get_t get) {
  free(get);
  return 0;
}

int32_t hb_get_set_row(hb_get_t get, unsigned char * row,
    size_t row_length) {
  return 0;
}

int32_t hb_get_set_table(hb_get_t get,
    char * table, size_t table_length) {
  return 0;
}

int32_t hb_get_set_namespace(hb_get_t get,
    char * name_space, size_t name_space_length) {
  return 0;
}

int32_t hb_get_send(hb_client_t client,
    hb_get_t get, hb_get_cb cb, void * extra) {
  if (cb) {
    cb(0, client, get, NULL, extra);
  }
  return 0;
}

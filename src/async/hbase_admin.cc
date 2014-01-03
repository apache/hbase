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

#include "async/hbase_admin.h"

#include <stdlib.h>
#include <stdbool.h>

#include "core/admin.h"
#include "async/hbase_connection.h"

int32_t hb_admin_create(hb_admin_t* admin_ptr,
    hb_connection_t connection) {
  (*admin_ptr) = reinterpret_cast<hb_admin_t>(new Admin());
  return 0;
}

/*
 * Disconnect the admin releasing any internal objects
 * or connections created in the background.
 */
int32_t hb_admin_destroy(hb_admin_t admin,
    hb_admin_disconnection_cb cb, void * extra) {
  if (cb)
    cb(0, admin, extra);
  free(admin);
  return 0;
}

/*
 * See if a table exists.
 */
int32_t hb_admin_table_exists(hb_admin_t admin,
    char * name_space, size_t name_space_length,
    char * table, size_t table_length,
    hb_admin_table_exists_cb cb, void * extra) {
  if (cb)
    cb(0, admin, name_space, name_space_length,
        table, table_length, true, extra);
  return 0;
}

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

#ifndef ASYNC_HBASE_ADMIN_H_
#define ASYNC_HBASE_ADMIN_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdbool.h>

#include "core/hbase_macros.h"
#include "async/hbase_connection.h"


typedef void (* hb_admin_disconnection_cb)( int32_t status,
    hb_admin_t admin, void * extra);
typedef void (* hb_admin_table_exists_cb)(int32_t status, hb_admin_t admin,
    char * name_space, size_t name_space_length,
    char * table, size_t table_length, bool exsists, void * extra);

/**
 * Create a new hb_admin.
 * All fields are initialized to the defaults. If you want to set
 * connection or other properties, set those before calling any
 * RPC functions.
 */
HBASE_API int32_t hb_admin_create(hb_admin_t* admin_ptr,
    hb_connection_t connection);

/*
 * Disconnect the admin releasing any internal objects
 * or connections created in the background.
 */
HBASE_API int32_t hb_admin_destroy(hb_admin_t admin,
    hb_admin_disconnection_cb cb, void * extra);

/*
 * See if a table exists.
 */
HBASE_API int32_t hb_admin_table_exists(hb_admin_t admin,
    char * name_space, size_t name_space_length,
    char * table, size_t table_length,
    hb_admin_table_exists_cb cb, void * extra);


#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // ASYNC_HBASE_ADMIN_H_

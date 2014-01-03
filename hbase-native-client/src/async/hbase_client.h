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

#ifndef ASYNC_HBASE_CLIENT_H_
#define ASYNC_HBASE_CLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "core/hbase_macros.h"
#include "core/hbase_types.h"

/*
 * Client disconnection callback typedef
 *
 * This is called after the connections are closed, but just
 * before the client is freed.
 */
typedef void (* hb_client_disconnection_cb)( int32_t status,
    hb_client_t client, void * extra);

/**
 * Create an hb_client_t.
 *
 * If connection is null then all defaults will be used.
 */
HBASE_API int32_t hb_client_create(hb_client_t * client_ptr,
    hb_connection_t connection);

/*
 * Disconnect the client releasing any internal objects
 * or connections created in the background.
 */
HBASE_API int32_t hb_client_destroy(hb_client_t client,
    hb_client_disconnection_cb cb, void * extra);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // ASYNC_HBASE_CLIENT_H_



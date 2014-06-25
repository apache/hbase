/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef LIBHBASE_CLIENT_H_
#define LIBHBASE_CLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Initializes a handle to hb_client_t which can be passed to other HBase APIs.
 * You need to use this method only once per HBase cluster. The returned handle
 * is thread safe.
 *
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_client_create(
    hb_connection_t connection, /* [in] hb_connection_t for this client */
    hb_client_t *client_ptr);   /* [out] pointer to hb_client_t handle */

/**
 * Flushes any buffered client-side write operations to HBase.
 * The callback will be invoked when everything that was buffered at the time of
 * the call has been flushed.
 * Note that this doesn't guarantee that ALL outstanding RPCs have completed.
 */
HBASE_API int32_t
hb_client_flush(
    hb_client_t client,
    hb_client_flush_cb cb,
    void *extra);

/**
 * Cleans up hb_client_t handle and release any held resources.
 * The callback is called after the connections are closed, but just before the
 * client is freed.
 */
HBASE_API int32_t
hb_client_destroy(
    hb_client_t client,
    hb_client_disconnection_cb cb,
    void *extra);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // LIBHBASE_CLIENT_H_

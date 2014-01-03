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

#ifndef ASYNC_HBASE_CONNECTION_H_
#define ASYNC_HBASE_CONNECTION_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "core/hbase_macros.h"
#include "core/hbase_types.h"
#include "core/hbase_connection_attr.h"

#include <stdlib.h>

/**
 * Create an hb_connection.
 *
 * if connection_attr is null everything will be left as default
 */
HBASE_API int32_t hb_connection_create(hb_connection_t * connection_ptr,
    hb_connection_attr_t connection_attr);

/**
 * Destroy the connection and free all resources allocated at creation
 * time.
 */
HBASE_API int32_t hb_connection_destroy(hb_connection_t connection);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // ASYNC_HBASE_CONNECTION_H_


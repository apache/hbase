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

#ifndef CORE_HBASE_CONNECTION_ATTR_H_
#define CORE_HBASE_CONNECTION_ATTR_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "core/hbase_macros.h"
#include "core/hbase_types.h"

#include <stdlib.h>

HBASE_API int32_t hb_connection_attr_create(hb_connection_attr_t * attr_ptr);

/**
 * Set the zk quorum of a connection that will be created.
 */
HBASE_API int32_t hb_connection_attr_set_zk_quorum(hb_connection_t connection,
    char * zk_quorum);

/**
 * Set the zk root of a connection that will be created.
 */
HBASE_API int32_t hb_connection_attr_set_zk_root(hb_connection_t connection,
    char * zk_root);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // CORE_HBASE_CONNECTION_ATTR_H_


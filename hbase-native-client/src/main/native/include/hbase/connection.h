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
#ifndef LIBHBASE_CONNECTION_H_
#define LIBHBASE_CONNECTION_H_

#include <stdint.h>

#include "types.h"

/**
 * Creates an hb_connection_t instance and initializes its address into
 * the passed pointer.
 */
HBASE_API int32_t
hb_connection_create(
    const char *zk_quorum,            /* [in] NULL terminated, comma separated
                                       *   string of Zookeeper servers. e.g.
                                       *   "<server1[:port]>,...". If set to
                                       *   NULL, library default will be used */
    const char *zk_root,              /* [in] The HBase Zookeeper root z-node.
                                       *   If set to NULL, library default will
                                       *   be used. */
    hb_connection_t *connection_ptr); /* [out] pointer to hb_connection_t */

/**
 * Destroy the connection and free all resources allocated at creation time.
 */
HBASE_API int32_t
hb_connection_destroy(
    hb_connection_t connection);  /* [in] hb_connection_t handle */

#endif /* LIBHBASE_CONNECTION_H_ */

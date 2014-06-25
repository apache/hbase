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
#ifndef LIBHBASE_ADMIN_H_
#define LIBHBASE_ADMIN_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Create a new hb_admin.
 * All fields are initialized to the defaults. If you want to set
 * connection or other properties, set those before calling any
 * RPC functions.
 */
HBASE_API int32_t
hb_admin_create(
    hb_connection_t connection,
    hb_admin_t *admin_ptr);

/**
 * Disconnect the admin releasing any internal objects
 * or connections created in the background.
 */
HBASE_API int32_t
hb_admin_destroy(
    hb_admin_t admin,
    hb_admin_disconnection_cb cb,
    void *extra);

/**
 * Checks if a table exists.
 * @returns 0 on if the table exist, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_exists(
    const hb_admin_t admin,   /* [in] HBaseClient handle */
    const char *name_space,   /* [in] Null terminated namespace, set to NULL
                               *   for default namespace and for 0.94 version */
    const char *table_name);  /* [in] Null terminated table name */

/**
 * Checks if a table is enabled.
 * @returns 0 on if the table enabled, HBASE_TABLE_DISABLED if the table
 * is disabled or an error code if an error occurs.
 */
HBASE_API int32_t
hb_admin_table_enabled(
    const hb_admin_t admin,   /* [in] HBaseClient handle */
    const char *name_space,   /* [in] Null terminated namespace, set to NULL
                               *   for default namespace and for 0.94 version */
    const char *table_name);  /* [in] Null terminated table name */

/**
 * Creates an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_create(
    const hb_admin_t admin,         /* [in] HBaseClient handle */
    const char *name_space,         /* [in] Null terminated namespace, set to NULL
                                     *   for default namespace and for 0.94 version */
    const char *table_name,         /* [in] Null terminated table name */
    const hb_columndesc families[], /* [in] Array of Null terminated family names */
    const size_t num_families);     /* [in] Number of families */

/**
 * Disable an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_disable(
    const hb_admin_t admin,  /* [in] HBaseClient handle */
    const char *name_space,  /* [in] Null terminated namespace, set to NULL
                              *   for default namespace and for 0.94 version */
    const char *table_name); /* [in] Null terminated table name */

/**
 * Enable an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_enable(
    const hb_admin_t admin,  /* [in] HBaseClient handle */
    const char *name_space,  /* [in] Null terminated namespace, set to NULL
                              *   for default namespace and for 0.94 version */
    const char *table_name); /* [in] Null terminated table name */

/**
 * Deletes an HBase table, disables the table if not already disabled.
 * Returns 0 on success, and error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_delete(
    const hb_admin_t admin , /* [in] HBaseClient handle */
    const char *name_space,  /* [in] Null terminated namespace, set to NULL
                              *   for default namespace and for 0.94 version */
    const char *table_name); /* [in] Null terminated table name */

#ifdef __cplusplus
}  // extern "C"
#endif

#endif /* LIBHBASE_ADMIN_H_ */

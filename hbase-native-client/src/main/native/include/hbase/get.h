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
#ifndef LIBHBASE_GET_H_
#define LIBHBASE_GET_H_

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Creates an hb_get_t object and populate the handle get_ptr.
 */
HBASE_API int32_t
hb_get_create(
    const byte_t *rk,    /* [in] the row key for this hb_get_t object */
    const size_t rk_len,
    hb_get_t *get_ptr);  /* [out] handle to an hb_get_t object */

/**
 * Queues the get request. Callback specified by cb will be called
 * on completion. Any buffer(s) attached to the get object can be
 * reclaimed only after the callback is received.
 */
HBASE_API int32_t
hb_get_send(
    hb_client_t client,
    hb_get_t get,
    hb_get_cb cb,
    void *extra);

/**
 * Sets the table name, required.
 */
HBASE_API int32_t
hb_get_set_table(
    hb_get_t get,
    const char *table_name,
    const size_t table_name_len);

/**
 * @NotYetImplemented
 *
 * Sets the table namespace (0.96 and later)
 */
HBASE_API int32_t
hb_get_set_namespace(
    hb_get_t get,
    const char *name_space,
    const size_t name_space_len);

/**
 * Release any resource held by hb_get_t object.
 */
HBASE_API int32_t
hb_get_destroy(hb_get_t get);

/**
 * Sets the row key, required.
 */
HBASE_API int32_t
hb_get_set_row(
    hb_get_t get,
    const byte_t *rk,
    const size_t rk_len);

/**
 * Adds a column family and optionally a column qualifier to
 * the hb_get_t object, optional.
 */
HBASE_API int32_t
hb_get_add_column(
    hb_get_t get,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len);

/**
 * @NotYetImplemented
 *
 * Optional. Adds a filter to the hb_get_t object.
 *
 * The filter must be specified using HBase Filter Language.
 * Refer to class org.apache.hadoop.hbase.filter.ParseFilter and
 * https://issues.apache.org/jira/browse/HBASE-4176 or
 * http://hbase.apache.org/book.html#thrift.filter-language for
 * language syntax and additional details.
 */
HBASE_API int32_t
hb_get_set_filter(
    hb_get_t get,
    const char *filter);

/**
 * Optional. Sets maximum number of latest values of each column to be
 * fetched.
 */
HBASE_API int32_t
hb_get_set_num_versions(
    hb_get_t get,
    const int32_t num_versions);

/**
 * @NotYetImplemented
 *
 * Optional. Only columns with the specified timestamp will be included.
 */
HBASE_API int32_t
hb_get_set_timestamp(
    hb_get_t get,
    const int64_t ts);

/**
 * @NotYetImplemented
 *
 * Optional. Only columns with timestamp within the specified range will
 * be included.
 */
HBASE_API int32_t
hb_get_set_timerange(
    hb_get_t get,
    const int64_t min_ts,
    const int64_t max_ts);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif  /* LIBHBASE_GET_H_ */

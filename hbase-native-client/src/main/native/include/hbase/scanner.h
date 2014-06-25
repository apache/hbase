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
#ifndef LIBHBASE_SCANNER_H_
#define LIBHBASE_SCANNER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Creates a client side row scanner. The returned scanner is not thread safe.
 * No RPC will be invoked until the call to fetch the next set of rows is made.
 * You can set the various attributes of this scanner until that point.
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_scanner_create(
    hb_client_t client,         /* [in] */
    hb_scanner_t *scanner_ptr); /* [out] */

/**
 * Request the next set of results from the server. You can set the maximum
 * number of rows returned by this call using hb_scanner_set_num_max_rows().
 */
HBASE_API int32_t
hb_scanner_next(
    hb_scanner_t scanner,
    hb_scanner_cb cb,
    void *extra);

/**
 * Close the scanner releasing any local and server side resources held.
 * The call back is fired just before the scanner's memory is freed.
 */
HBASE_API int32_t
hb_scanner_destroy(
    hb_scanner_t scanner,
    hb_scanner_destroy_cb cb,
    void *extra);

/**
 * Set the table name for the scanner
 */
HBASE_API int32_t
hb_scanner_set_table(
    hb_scanner_t scanner,
    const char *table,
    const size_t table_length);

/**
 * Set the name space for the scanner (0.96 and above)
 */
HBASE_API int32_t
hb_scanner_set_namespace(
    hb_scanner_t scanner,
    const char *name_space,
    const size_t name_space_length);

/**
 * Specifies the start row key for this scanner (inclusive).
 */
HBASE_API int32_t
hb_scanner_set_start_row(
    hb_scanner_t scanner,
    const byte_t *start_row,
    const size_t start_row_length);

/**
 * Specifies the end row key for this scanner (exclusive).
 */
HBASE_API int32_t
hb_scanner_set_end_row(
    hb_scanner_t scanner,
    const byte_t *end_row,
    const size_t end_row_length);

/**
 * Sets the maximum versions of a column to fetch.
 */
HBASE_API int32_t
hb_scanner_set_num_versions(
    hb_scanner_t scanner,
    const int8_t num_versions);

/**
 * Sets the maximum number of rows to scan per call to hb_scanner_next().
 */
HBASE_API int32_t
hb_scanner_set_num_max_rows(
    hb_scanner_t scanner,
    const size_t cache_size);

/**
 * @NotYetImplemented
 *
 * Optional. Adds a filter to the hb_scanner_t object.
 *
 * The filter must be specified using HBase Filter Language.
 * Refer to class org.apache.hadoop.hbase.filter.ParseFilter and
 * https://issues.apache.org/jira/browse/HBASE-4176 or
 * http://hbase.apache.org/book.html#thrift.filter-language for
 * language syntax and additional details.
 */
HBASE_API int32_t
hb_scanner_set_filter(
    hb_scanner_t scanner,
    const char *filter);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif  /* LIBHBASE_SCANNER_H_*/

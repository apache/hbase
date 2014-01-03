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

#ifndef ASYNC_HBASE_SCANNER_H_
#define ASYNC_HBASE_SCANNER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "async/hbase_result.h"
#include "core/hbase_types.h"

HBASE_API int32_t hb_scanner_create(hb_scanner_t * scanner_ptr);

HBASE_API int32_t hb_scanner_set_table(hb_scanner_t scanner,
    char * table, size_t table_length);
HBASE_API int32_t hb_scanner_set_namespace(hb_scanner_t scanner,
    char * name_space, size_t name_space_length);

HBASE_API int32_t hb_scanner_set_start_row(hb_scanner_t scanner,
    unsigned char * start_row, size_t start_row_length);
HBASE_API int32_t hb_scanner_set_end_row(hb_scanner_t scanner,
    unsigned char * end_row, size_t end_row_length);

HBASE_API int32_t hb_scanner_set_cache_size(hb_scanner_t scanner,
    size_t cache_size);
HBASE_API int32_t hb_scanner_set_batch_size(hb_scanner_t scanner,
    size_t batch_size);
HBASE_API int32_t hb_scanner_set_num_versions(hb_scanner_t scanner,
    int8_t num_versions);

/*
 * Scanner call back typedef.
 *
 * This will be called when initinalization of the scanner
 * is complete.  It will also be called when scanner next
 * returns results.
 */
typedef void (* hb_scanner_cb)(int32_t status,
                               hb_client_t client,
                               hb_scanner_t scanner,
                               hb_result_t results,
                               size_t num_results,
                               void * extra);
/*
 * Get the next results from the scanner
 */
HBASE_API int32_t hb_scanner_next(hb_client_t client,
    hb_scanner_t scanner, hb_scanner_cb cb, void * extra);

/*
 * Close the scanner releasing any local and server side
 * resources held. The call back is fired just before the
 * scanner's memory is freed.
 */
HBASE_API int32_t hb_scanner_destroy(hb_client_t client,
    hb_scanner_t scanner, hb_scanner_cb cb, void * extra);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // ASYNC_HBASE_SCANNER_H_

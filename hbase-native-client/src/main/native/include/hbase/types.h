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
#ifndef LIBHBASE_TYPES_H_
#define LIBHBASE_TYPES_H_

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "macros.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * HBase ubiquitous byte type
 */
#ifdef byte_t
#undef byte_t
#endif
typedef uint8_t byte_t;

/**
 * Base HBase Cell type.
 */
typedef struct hb_cell_type {
  /* row key */
  byte_t *row;
  size_t  row_len;

  /* column family */
  byte_t *family;
  size_t  family_len;

  /* column qualifier */
  byte_t *qualifier;
  size_t  qualifier_len;

  /* column value */
  byte_t *value;
  size_t  value_len;

  /* timestamp */
  int64_t ts;

  /* for internal use, applications should not set or alter this variable */
  void   *private_;
} hb_cell_t;

#define HBASE_LATEST_TIMESTAMP  0x7fffffffffffffffL

typedef void *hb_connection_t;
typedef void *hb_client_t;
typedef void *hb_admin_t;

typedef void *hb_table_t;
typedef void *hb_get_t;
typedef void *hb_result_t;
typedef void *hb_mutation_t;
typedef void *hb_put_t;
typedef void *hb_delete_t;
typedef void *hb_increment_t;
typedef void *hb_append_t;
typedef void *hb_scanner_t;
typedef void *hb_columndesc;

typedef enum {
  DURABILITY_USE_DEFAULT = 0, /* Use column family's default setting */
  DURABILITY_SKIP_WAL    = 1, /* Do not write the Mutation to the WAL */
  DURABILITY_ASYNC_WAL   = 2, /* Write the Mutation to the WAL asynchronously */
  DURABILITY_SYNC_WAL    = 3, /* Write the Mutation to the WAL synchronously */
  DURABILITY_FSYNC_WAL   = 4  /* Write the Mutation to the WAL synchronously and force
                               * the entries to disk. (Note: this is currently not
                               * supported and will behave identical to SYNC_WAL)
                               * See https://issues.apache.org/jira/browse/HADOOP-6313 */
} hb_durability_t;

/**
 *  Log levels
 */
typedef enum {
  HBASE_LOG_LEVEL_INVALID = 0,
  HBASE_LOG_LEVEL_FATAL   = 1,
  HBASE_LOG_LEVEL_ERROR   = 2,
  HBASE_LOG_LEVEL_WARN    = 3,
  HBASE_LOG_LEVEL_INFO    = 4,
  HBASE_LOG_LEVEL_DEBUG   = 5,
  HBASE_LOG_LEVEL_TRACE   = 6
} HBaseLogLevel;

/*******************************************************************************
 *                            HBase Error Codes                                *
 *******************************************************************************
 * The HBase APIs returns or invoke the callback functions with error code     *
 * set to 0. A non-zero value indicates an error condition.                    *
 *                                                                             *
 * EAGAIN   A recoverable error occurred and the operation can be immediately  *
 *          retired.                                                           *
 *                                                                             *
 * ENOBUFS  The resources required to complete the operations are temporarily  *
 *          exhausted and the application should retry the operation after a   *
 *          brief pause, possibly throttling the rate of requests.             *
 *                                                                             *
 * ENOENT   The requested table or a column family was not found.              *
 *                                                                             *
 * EEXIST   Table or column family already exist.                              *
 *                                                                             *
 * ENOMEM   The process ran out of memory while trying to process the request. *
 *                                                                             *
 * HBASE_TABLE_DISABLED                                                        *
 *          The table is disabled.                                             *
 *                                                                             *
 * HBASE_TABLE_NOT_DISABLED                                                    *
 *          The table is not disabled.                                         *
 *                                                                             *
 * HBASE_UNKNOWN_SCANNER                                                       *
 *          The specified scanner does not exist on region server.             *
 *                                                                             *
 * HBASE_INTERNAL_ERR                                                          *
 *          An internal error for which there is no error code defined. More   *
 *          information will be available in the log.                          *
 *                                                                             *
 *******************************************************************************/
/**
 * HBase custom error codes
 */
#define HBASE_INTERNAL_ERR          -10000
#define HBASE_TABLE_DISABLED        (HBASE_INTERNAL_ERR-1)
#define HBASE_TABLE_NOT_DISABLED    (HBASE_INTERNAL_ERR-2)
#define HBASE_UNKNOWN_SCANNER       (HBASE_INTERNAL_ERR-3)

/*******************************************************************************
 *                            HBase API Callbacks                              *
 *******************************************************************************
 *      The following section defines various callback functions for the async *
 * APIs.  These callbacks may be triggered on the same thread as the caller or *
 * on another thread. The application is expected to not block in the callback *
 * routines.                                                                   *
 *******************************************************************************/

/**
 * HBase Admin disconnection callback typedef
 *
 * This is called after the connections are closed, but just before
 * the client is freed.
 *
 * Refer to the section on error code for the list of possible values
 * for 'err'. A value of 0 indicates success.
 */
typedef void (*hb_admin_disconnection_cb)(
    int32_t err,
    hb_admin_t admin,
    void *extra);

/**
 * Client disconnection callback typedef
 * This is called after the connections are closed, but just before
 * the client is freed.
 *
 * Refer to the section on error code for the list of possible values
 * for 'err'. A value of 0 indicates success.
 */
typedef void (*hb_client_disconnection_cb) (
      int32_t err,
      hb_client_t client,
      void *extra);

/**
 * Client flush callback typedef
 * This will be invoked when everything that was buffered at the time of
 * the call has been flushed.
 *
 * Refer to the section on error code for the list of possible values
 * for 'err'. A value of 0 indicates success.
 */
typedef void (*hb_client_flush_cb) (
      int32_t err,
      hb_client_t client,
      void *extra);

/**
 * Mutation call back typedef
 *
 * This callback is triggered with result of each mutation.
 *
 * Refer to the section on error code for the list of possible values
 * for 'err'. A value of 0 indicates success.
 */
typedef void (*hb_mutation_cb)(
      int32_t err,
      hb_client_t client,
      hb_mutation_t mutation,
      hb_result_t result,
      void *extra);

/**
 * Get callback typedef.
 * This callback is triggered with result of each get call.
 *
 * The result, if not NULL, must be freed by calling hb_result_destroy().
 *
 * Refer to the section on error code for the list of possible values
 * for 'err'. A value of 0 indicates success.
 */
typedef void (*hb_get_cb)(
      int32_t err,
      hb_client_t client,
      hb_get_t get,
      hb_result_t result,
      void *extra);

/**
 * Scanner call back typedef.
 * This will be called when scanner next returns results.
 *
 * The individual results in the results array must be freed by calling
 * hb_result_destroy(). The array itself is freed once the callback returns.
 *
 * Refer to the section on error code for the list of possible values
 * for 'err'. A value of 0 indicates success.
 */
typedef void (*hb_scanner_cb)(
      int32_t err,
      hb_scanner_t scanner,
      hb_result_t results[],
      size_t num_results,
      void *extra);

/**
 * Scanner close callback.
 *
 * Refer to the section on error code for the list of possible values
 * for 'err'. A value of 0 indicates success.
 */
typedef void (*hb_scanner_destroy_cb)(
      int32_t err,
      hb_scanner_t scanner,
      void *extra);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* LIBHBASE_TYPES_H_ */

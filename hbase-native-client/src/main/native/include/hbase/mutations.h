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
#ifndef LIBHBASE_MUTATIONS_H_
#define LIBHBASE_MUTATIONS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Creates a structure for put operation and return its handle.
 */
HBASE_API int32_t
hb_put_create(
    const byte_t *rowkey,
    const size_t rowkey_len,
    hb_put_t *put_ptr);

/**
 * Creates a structure for delete operation and return its handle.
 */
HBASE_API int32_t
hb_delete_create(
    const byte_t *rowkey,
    const size_t rowkey_len,
    hb_delete_t *delete_ptr);

/**
 * @NotYetImplemented
 *
 * Creates a structure for increment operation and return its handle.
 */
HBASE_API int32_t
hb_increment_create(
    const byte_t *rowkey,
    const size_t rowkey_len,
    hb_increment_t *increment_ptr);

/**
 * @NotYetImplemented
 *
 * Creates an structure for append operation and return its handle.
 */
HBASE_API int32_t
hb_append_create(
    const byte_t *rowkey,
    const size_t rowkey_len,
    hb_append_t *append_ptr);

/**
 * Queue a mutation to go out. These mutations will not be performed
 * atomically and can be batched in a non-deterministic way on either
 * the server or the client side.
 *
 * Any buffer attached to the the mutation objects must not be altered
 * until the callback has been received.
 */
HBASE_API int32_t
hb_mutation_send(
    hb_client_t client,
    hb_mutation_t mutation,
    hb_mutation_cb cb,
    void *extra);

/**
 * Frees up any resource held by the mutation structure.
 */
HBASE_API int32_t
hb_mutation_destroy(hb_mutation_t mutation);

// Shared setters.
/**
 * @NotYetImplemented
 *
 * Sets the namespace for the mutation (0.96 and later).
 */
HBASE_API int32_t
hb_mutation_set_namespace(
    hb_mutation_t mutation,
    const char *name_space,
    const size_t name_space_len);

/**
 * Sets the table name for the mutation.
 */
HBASE_API int32_t
hb_mutation_set_table(
    hb_mutation_t mutation,
    const char *table,
    const size_t table_len);

/**
 * Sets whether or not this RPC can be buffered on the client side.
 *
 * Currently only puts and deletes can be buffered. Calling this for
 * any other mutation type will return EINVAL.
 *
 * The default is true.
 */
HBASE_API int32_t
hb_mutation_set_bufferable(
    hb_mutation_t mutation,
    const bool bufferable);

/**
 * Sets the row key for the mutation.
 */
HBASE_API int32_t
hb_mutation_set_row(
    hb_mutation_t mutation,
    const byte_t *rowkey,
    const size_t rowkey_len);

/**
 * Sets the durability guarantees for the Mutation.
 */
HBASE_API int32_t
hb_mutation_set_durability(
    hb_mutation_t mutation,
    const hb_durability_t durability);

/**
 * Adds a cell to the put structure. The row key of the cell
 * must be same as the row key of the put structure.
 */
HBASE_API int32_t
hb_put_add_cell(
    hb_put_t put,
    const hb_cell_t *cell);

/**
 * Adds a column (key-value) with latest timestamp to the put.
 */
HBASE_API int32_t
hb_put_add_column(
    hb_put_t put,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    const byte_t *value,
    const size_t value_len);

/**
 * Adds a column (key-value) with the specified timestamp to the put.
 */
HBASE_API int32_t
hb_put_add_ts_column(
    hb_put_t put,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    const byte_t *value,
    const size_t value_len,
    const int64_t timestamp);

/**
 * Optional. Set the column criteria for hb_delete_t object.
 * Set the qualifier to NULL to delete all columns of a family.
 * Only the cells with timestamp less than or equal to the specified
 * timestamp are deleted. Set the timestamp to -1 to delete all
 * versions of the column.
 */
HBASE_API int32_t
hb_delete_add_column(
    hb_delete_t del,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    const int64_t timestamp);

/**
 * Optional. Sets the timestamp of the delete row or delete family
 * operation.
 * This does not override the timestamp of individual columns
 * added via hb_delete_add_column().
 */
HBASE_API int32_t
hb_delete_set_timestamp(
    hb_delete_t del,
    const int64_t timestamp);

/**
 * @NotYetImplemented
 *
 * Add a column and the amount by which its value to be incremented
 * to the increment operation.
 */
HBASE_API int32_t
hb_increment_add_column(
    hb_increment_t incr,
    const hb_cell_t *cell,
    const int64_t amount);

/**
 * @NotYetImplemented
 *
 * Add a column for the append operation.
 */
HBASE_API int32_t
hb_append_add_column(
    hb_append_t append,
    const hb_cell_t *cell);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // LIBHBASE_MUTATIONS_H_

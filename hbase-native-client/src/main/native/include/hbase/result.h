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
#ifndef LIBHBASE_RESULT_H_
#define LIBHBASE_RESULT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Returns the row key of this hb_result_t object.
 * This buffer is valid until hb_result_destroy() is called.
 * Callers should not modify this buffer.
 */
HBASE_API int32_t
hb_result_get_key(
    const hb_result_t result, /* [in] */
    const byte_t **key_ptr,   /* [out] */
    size_t *key_length_ptr);  /* [out] */

/**
 * Returns the table name of this hb_result_t object.
 * This buffer is valid until hb_result_destroy() is called.
 * Callers should not modify this buffer.
 */
HBASE_API int32_t
hb_result_get_table(
    const hb_result_t result,  /* [in] */
    const char **table_ptr,    /* [out] */
    size_t *table_length_ptr); /* [out] */

/**
 * HBase 0.96 or later.
 * Returns the namespace of this hb_result_t object.
 * This buffer is valid until hb_result_destroy() is called.
 * Callers should not modify this buffer.
 */
HBASE_API int32_t
hb_result_get_namespace(
    const hb_result_t result,      /* [in] */
    const char **namespace_ptr,    /* [out] */
    size_t *namespace_length_ptr); /* [out] */

/**
 * Returns the total number of cells in this hb_result_t object.
 */
HBASE_API int32_t
hb_result_get_cell_count(
    const hb_result_t result, /* [in] */
    size_t *cell_count_ptr);  /* [out] */

/**
 * Returns the pointer to a constant hb_cell_t structure with the most recent
 * value of the given column. The buffers are valid until hb_result_destroy()
 * is called. Callers should not modify these buffers.
 *
 * @returns 0       if operation succeeds.
 * @returns ENOENT  if a matching cell is not found.
 */
HBASE_API int32_t
hb_result_get_cell(
    const hb_result_t result,    /* [in] */
    const byte_t *family,        /* [in] */
    const size_t family_len,     /* [in] */
    const byte_t *qualifier,     /* [in] */
    const size_t qualifier_len,  /* [in] */
    const hb_cell_t **cell_ptr); /* [out] */

/**
 * Returns the pointer to a constant hb_cell_t structure containing the cell
 * value at the given 0 based index of the result. The buffers are valid until
 * hb_result_destroy() is called. Callers should not modify these buffers.
 *
 * @returns 0       if operation succeeds.
 * @returns ERANGE  if the index is outside the bounds.
 */
HBASE_API int32_t
hb_result_get_cell_at(
    const hb_result_t result,    /* [in] */
    const size_t index,          /* [in] */
    const hb_cell_t **cell_ptr); /* [out] */

/**
 * Returns the array of pointers to constant hb_cell_t structures with the cells
 * of the result. The buffers are valid until hb_result_destroy() is called. The
 * variable pointed by num_cells_ptr is set to the number of cells in the result.
 *
 * Calling this function multiple times for the same hb_result_t may return
 * the same buffers. Callers should not modify these buffers.
 */
HBASE_API int32_t
hb_result_get_cells(
    const hb_result_t result,     /* [in] */
    const hb_cell_t ***cells_ptr, /* [out] */
    size_t *num_cells_ptr);       /* [out] */

/**
 * Frees any resources held by the hb_result_t object.
 */
HBASE_API int32_t
hb_result_destroy(hb_result_t result);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif  /* LIBHBASE_RESULT_H_*/

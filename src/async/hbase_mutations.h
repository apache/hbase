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

#ifndef ASYNC_HBASE_MUTATIONS_H_
#define ASYNC_HBASE_MUTATIONS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "core/hbase_types.h"
#include "async/hbase_result.h"

// Creation methods

/**
 * Create a put.
 * Ownership passes to the caller.
 */
HBASE_API int32_t hb_put_create(hb_put_t * put_ptr);

/**
 * Create a delete
 * Ownership passes to the caller.
 */
HBASE_API int32_t hb_delete_create(hb_delete_t * delete_ptr);

/**
 * Create an increment
 * Ownership passes to the caller.
 */
HBASE_API int32_t hb_increment_create(hb_increment_t * increment_ptr);

/**
 * Create an append
 * Ownership passes to the caller.
 */
HBASE_API int32_t hb_append_create(hb_append_t * append_ptr);

/**
 * Destroy the mutation.
 * All internal structures are cleaned up.  However any backing
 * data structures passed in by the user are not cleaned up.
 */
HBASE_API int32_t hb_mutation_destroy(hb_mutation_t mutation);

// Shared setters.
HBASE_API int32_t hb_mutation_set_namespace(hb_mutation_t mutation,
    char * name_space, size_t name_space_length);
HBASE_API int32_t hb_mutation_set_table(hb_mutation_t mutation,
    char * table, size_t table_length);
HBASE_API int32_t hb_mutation_set_row(hb_mutation_t mutation,
    unsigned char * rk, size_t row_length);
HBASE_API int32_t hb_mutation_set_durability(hb_mutation_t mutation,
    hb_durability_type durability);

// Put Setters etc.
HBASE_API int32_t hb_put_add_cell(hb_put_t put, hb_cell_t * cell);

// Delete
HBASE_API int32_t hb_delete_add_col(hb_increment_t incr,
    unsigned char * family, size_t family_length,
    unsigned char * qual, size_t qual_length);

// Increment
HBASE_API int32_t hb_increment_add_value(hb_increment_t incr,
    unsigned char * family, size_t family_length,
    unsigned char * qual, size_t qual_length,
    int64_t ammount);

// Append
HBASE_API int32_t hb_append_add_cell(hb_append_t put, hb_cell_t * cell);

// Now that the mutations are created and populated
// The real meat of the client is below.

/*
 * mutation call back typedef
 */
typedef void (* hb_mutation_cb)(int32_t status,
    hb_client_t client, hb_mutation_t mutation,
    hb_result_t result, void * extra);

/*
 * Queue a single mutation.  This mutation will be
 * sent out in the background and can be batched with
 * other requests destined for the same server.
 *
 * The call back will be executed after the response
 * is received from the RegionServer.  Even if the
 * mutation was batched with other requests,
 * the call back will be invoked for every mutation
 * individually.
 */
HBASE_API int32_t hb_mutation_send(hb_client_t client,
    hb_mutation_t mutation, hb_mutation_cb cb,
    void * extra);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // ASYNC_HBASE_MUTATIONS_H_

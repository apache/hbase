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

#ifndef CORE_HBASE_TYPES_H_
#define CORE_HBASE_TYPES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

typedef unsigned char hb_byte_t;

/*
 * Base kv type.
 */
typedef struct {
  hb_byte_t* row;
  size_t row_length;

  char * family;
  size_t family_length;

  hb_byte_t* qual;
  size_t qual_length;

  hb_byte_t* value;
  size_t value_length;

  uint64_t timestamp;
} hb_cell_t;

typedef enum {
  DELETE_ONE_VERSION,
  DELETE_MULTIPLE_VERSIONS,
  DELETE_FAMILY,
  DELETE_FAMILY_VERSION
} hb_delete_type;

typedef enum {
  USE_DEFAULT,
  SKIP_WAL,
  ASYNC_WAL,
  SYNC_WAL,
  HSYNC_WAL
} hb_durability_type;

typedef void* hb_admin_t;
typedef void* hb_client_t;
typedef void* hb_connection_attr_t;
typedef void* hb_connection_t;
typedef void* hb_get_t;
typedef void* hb_mutation_t;
typedef void* hb_put_t;
typedef void* hb_delete_t;
typedef void* hb_increment_t;
typedef void* hb_append_t;
typedef void* hb_result_t;
typedef void* hb_scanner_t;

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // CORE_HBASE_TYPES_H_

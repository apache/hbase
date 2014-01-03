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
#include "async/hbase_scanner.h"

#include <stdlib.h>

#include "core/hbase_types.h"
#include "core/scanner.h"

int32_t hb_scanner_create(hb_scanner_t * scanner_ptr) {
  (*scanner_ptr) = reinterpret_cast<hb_scanner_t>(new Scanner());
  return (*scanner_ptr != NULL)?0:1;
}

HBASE_API int32_t hb_scanner_set_table(hb_scanner_t scanner,
    char * table, size_t table_length) {
  return 0;
}

HBASE_API int32_t hb_scanner_set_namespace(hb_scanner_t scanner,
    char * name_space, size_t name_space_length) {
  return 0;
}

int32_t hb_scanner_set_start_row(hb_scanner_t scanner,
    unsigned char * start_row, size_t start_row_length) {
  return 0;
}

int32_t hb_scanner_set_end_row(hb_scanner_t scanner,
    unsigned char * end_row, size_t end_row_length) {
  return 0;
}

int32_t hb_scanner_set_cache_size(hb_scanner_t scanner,
    size_t cache_size) {
  return 0;
}

int32_t hb_scanner_set_num_versions(hb_scanner_t scanner,
    int8_t num_versions) {
  return 0;
}

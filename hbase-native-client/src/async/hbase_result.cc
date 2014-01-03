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

#include "async/hbase_result.h"

#include "core/hbase_types.h"

int32_t hb_result_get_cells(hb_result_t result,
    hb_cell_t ** cell_ptr, size_t * num_cells) {
  return 0;
}

int32_t hb_result_get_table(hb_result_t result,
    char ** table, size_t * table_length) {
  return 0;
}

int32_t hb_result_get_namespace(hb_result_t result,
    char ** name_space, size_t * name_space_length) {
  return 0;
}

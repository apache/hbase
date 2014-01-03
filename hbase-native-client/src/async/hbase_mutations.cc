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

#include "async/hbase_mutations.h"

#include <stdlib.h>

#include "core/hbase_types.h"
#include "core/mutation.h"
#include "core/put.h"
#include "core/delete.h"

#include "async/hbase_result.h"

extern "C" {
int32_t hb_put_create(hb_put_t* put_ptr) {
  (*put_ptr) = reinterpret_cast<hb_put_t>(new Put());
  return 0;
}

int32_t hb_delete_create(hb_delete_t * delete_ptr) {
  (*delete_ptr) = reinterpret_cast<hb_delete_t>(new Delete());
  return 0;
}

int32_t hb_increment_create(hb_increment_t * increment_ptr) {
  return 0;
}

int32_t hb_append_create(hb_append_t * append_ptr) {
  return 0;
}

int32_t hb_mutation_destroy(hb_mutation_t mutation) {
  return 0;
}

HBASE_API int32_t hb_mutation_set_namespace(hb_mutation_t mutation,
    char * name_space, size_t name_space_length) {
  Mutation * m = reinterpret_cast<Mutation *>(mutation);
  m->set_namespace(name_space, name_space_length);
  return 0;
}

HBASE_API int32_t hb_mutation_set_table(hb_mutation_t mutation,
    char * table, size_t table_length) {
  Mutation * m = reinterpret_cast<Mutation *>(mutation);
  m->set_namespace(table, table_length);
  return 0;
}

int32_t hb_mutation_set_row(hb_mutation_t mutation,
    unsigned char * rk, size_t row_length) {
  Mutation * m = reinterpret_cast<Mutation *>(mutation);
  m->set_row(rk, row_length);
  return 0;
}

int32_t hb_mutation_set_durability(hb_mutation_t mutation,
    hb_durability_type durability) {
  Mutation * m = reinterpret_cast<Mutation *>(mutation);
  m->set_durability(durability);
  return 0;
}

int32_t hb_put_add_cell(hb_put_t put, hb_cell_t * cell) {
  return 0;
}

int32_t hb_delete_add_col(hb_increment_t incr,
    unsigned char * family, size_t family_length,
    unsigned char * qual, size_t qual_length) {
  return 0;
}

int32_t hb_increment_add_value(hb_increment_t incr,
    unsigned char * family, size_t family_length,
    unsigned char * qual, size_t qual_length,
    int64_t ammount) {
  return 0;
}

int32_t hb_append_add_cell(hb_append_t put, hb_cell_t * cell) {
  return 0;
}

int32_t hb_mutation_send(hb_client_t client,
    hb_mutation_t mutation, hb_mutation_cb cb,
    void * extra) {
  if (cb) {
    cb(0, client, mutation, NULL, extra);
  }
  return 0;
}
}   // extern "C"

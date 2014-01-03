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

#ifndef CORE_MUTATION_H_
#define CORE_MUTATION_H_

#include <stdlib.h>

#include "core/hbase_types.h"

class Mutation {
  char * name_space;
  size_t name_space_length;

  char * table;
  size_t table_length;

  unsigned char * row;
  size_t row_length;

  hb_durability_type durability;
 public:
  void set_namespace(char * name_space, size_t name_space_length);
  void set_table(char * table, size_t table_length);
  void set_row(unsigned char * row, size_t row_length);
  void set_durability(hb_durability_type durability);

  virtual ~Mutation();
};
#endif  // CORE_MUTATION_H_



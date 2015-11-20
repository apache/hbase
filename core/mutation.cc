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

#include "core/mutation.h"

void Mutation::set_namespace(char *name_space, size_t name_space_length) {
  this->name_space = name_space;
  this->name_space_length = name_space_length;
}

void Mutation::set_table(char *table, size_t table_length) {
  this->table = table;
  this->table_length = table_length;
}

void Mutation::set_row(unsigned char *row, size_t row_length) {
  this->row = row;
  this->row_length = row_length;
}

void Mutation::set_durability(durability_type durability) {
  this->durability = durability;
}

Mutation::~Mutation() {}

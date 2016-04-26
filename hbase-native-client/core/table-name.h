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
#pragma once

#include <memory>
#include <string>

#include <folly/Conv.h>

namespace hbase {

// This is the core class of a HBase client.
class TableName {
public:
  explicit TableName(std::string table_name);
  explicit TableName(std::string name_space, std::string table_name);

  std::string name_space() const { return name_space_; };
  std::string table() const { return table_; };
  bool is_default_name_space() const;
  bool operator==(const TableName &other) const;

private:
  std::string name_space_;
  std::string table_;
};

// Provide folly::to<std::string>(TableName);
template <class String> void toAppend(const TableName &in, String *result) {
  if (in.is_default_name_space()) {
    folly::toAppend(in.table(), result);
  } else {
    folly::toAppend(in.name_space(), ':', in.table(), result);
  }
}

} // namespace hbase

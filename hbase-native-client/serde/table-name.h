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
#include <folly/String.h>
#include "if/HBase.pb.h"

namespace hbase {
namespace pb {

// Provide folly::to<std::string>(TableName);
template <class String>
void toAppend(const TableName &in, String *result) {
  if (!in.has_namespace_() || in.namespace_() == "default") {
    folly::toAppend(in.qualifier(), result);
  } else {
    folly::toAppend(in.namespace_(), ':', in.qualifier(), result);
  }
}

template <class String>
void parseTo(String in, TableName &out) {
  std::vector<std::string> v;
  folly::split(":", in, v);

  if (v.size() == 1) {
    out.set_namespace_("default");
    out.set_qualifier(v[0]);
  } else {
    out.set_namespace_(v[0]);
    out.set_qualifier(v[1]);
  }
}

}  // namespace pb
}  // namespace hbase

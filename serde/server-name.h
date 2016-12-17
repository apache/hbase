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

#include <folly/Conv.h>
#include <folly/String.h>

#include <string>

#include "if/HBase.pb.h"

namespace hbase {
namespace pb {

template <class String>
void parseTo(String in, ServerName &out) {
  // TODO see about getting rsplit into folly.
  std::string s = folly::to<std::string>(in);

  auto delim = s.rfind(":");
  if (delim == std::string::npos) {
    throw std::runtime_error("Couldn't parse server name");
  }
  out.set_host_name(s.substr(0, delim));
  // Now keep everything after the : (delim + 1) to the end.
  out.set_port(folly::to<int>(s.substr(delim + 1)));
}

}  // namespace pb
}  // namespace hbase

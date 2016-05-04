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

#include "if/HBase.pb.h"

#include <boost/algorithm/string/predicate.hpp>
#include <folly/Conv.h>

namespace hbase {
namespace pb {
template <class String> void parseTo(String in, RegionInfo &out) {
  // TODO(eclark): there has to be something better.
  std::string s = folly::to<std::string>(in);

  if (!boost::starts_with(s, "PBUF")) {
    throw std::runtime_error("Region Info field doesn't contain preamble");
  }
  if (!out.ParseFromArray(s.data() + 4, s.size() - 4)) {
    throw std::runtime_error("Bad protobuf for RegionInfo");
  }
}
} // namespace pb
} // namespace hbase

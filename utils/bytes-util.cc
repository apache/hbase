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

#include "utils/bytes-util.h"

#include <memory>
#include <string>

#include <glog/logging.h>

namespace hbase {

constexpr char BytesUtil::kHexChars[];

std::string BytesUtil::ToStringBinary(const std::string& b, size_t off, size_t len) {
  std::string result;
  // Just in case we are passed a 'len' that is > buffer length...
  if (off >= b.size()) {
    return result;
  }
  if (off + len > b.size()) {
    len = b.size() - off;
  }
  for (size_t i = off; i < off + len; ++i) {
    int32_t ch = b[i] & 0xFF;
    if (ch >= ' ' && ch <= '~' && ch != '\\') {
      result += ch;
    } else {
      result += "\\x";
      result += kHexChars[ch / 0x10];
      result += kHexChars[ch % 0x10];
    }
  }
  return result;
}

} /* namespace hbase */

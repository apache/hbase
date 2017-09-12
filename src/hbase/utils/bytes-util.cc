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

#include "hbase/utils/bytes-util.h"

#include <bits/stdc++.h>
#include <boost/predef.h>
#include <glog/logging.h>

#include <memory>
#include <string>

namespace hbase {

constexpr char BytesUtil::kHexChars[];

std::string BytesUtil::ToString(int64_t val) {
  std::string res;
#if BOOST_ENDIAN_BIG_BYTE || BOOST_ENDIAN_BIG_WORD
  for (int i = 7; i > 0; i--) {
    res += (int8_t)(val & 0xffu);
    val = val >> 8;
  }
  res += (int8_t)val;
#else
  int64_t mask = 0xff00000000000000u;
  for (int i = 56; i >= 1; i -= 8) {
    auto num = ((val & mask) >> i);
    res += num;
    mask = mask >> 8;
  }
  res += (val & 0xff);
#endif
  return res;
}

int64_t BytesUtil::ToInt64(std::string str) {
  if (str.length() < 8) {
    throw std::runtime_error("There are not enough bytes. Expected: 8, actual: " + str.length());
  }
  const unsigned char *bytes = reinterpret_cast<unsigned char *>(const_cast<char *>(str.c_str()));
  int64_t l = 0;
  for (int i = 0; i < 8; i++) {
    l <<= 8;
    l ^= bytes[i];
  }
  return l;
}

std::string BytesUtil::ToStringBinary(const std::string &b, size_t off, size_t len) {
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

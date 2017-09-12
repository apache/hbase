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

namespace hbase {

class BytesUtil {
 private:
  static const constexpr char kHexChars[] = "0123456789ABCDEF";

 public:
  static std::string ToStringBinary(const std::string& b) { return ToStringBinary(b, 0, b.size()); }
  /**
    * Write a printable representation of a byte array. Non-printable
    * characters are hex escaped in the format \\x%02X, eg:
    * \x00 \x05 etc
    *
    * @param b array to write out
    * @param off offset to start at
    * @param len length to write
    * @return string output
    */
  static std::string ToStringBinary(const std::string& b, size_t off, size_t len);

  static std::string ToString(int64_t value);

  static int64_t ToInt64(std::string str);

  static bool IsEmptyStartRow(const std::string& row) { return row == ""; }

  static bool IsEmptyStopRow(const std::string& row) { return row == ""; }

  static int32_t CompareTo(const std::string& a, const std::string& b) {
    if (a < b) {
      return -1;
    }
    if (a == b) {
      return 0;
    }
    return 1;
  }

  /**
   * Create the closest row after the specified row
   */
  static std::string CreateClosestRowAfter(std::string row) { return row.append(1, '\0'); }
};
} /* namespace hbase */

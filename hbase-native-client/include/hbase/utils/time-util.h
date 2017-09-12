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

#include <chrono>
#include <string>

namespace hbase {

class TimeUtil {
 public:
  static inline int64_t ToMillis(const int64_t& nanos) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(nanos))
        .count();
  }

  static inline std::chrono::milliseconds ToMillis(const std::chrono::nanoseconds& nanos) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(nanos));
  }

  static inline std::chrono::nanoseconds ToNanos(const std::chrono::milliseconds& millis) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(millis);
  }

  static inline std::chrono::nanoseconds MillisToNanos(const int64_t& millis) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(millis));
  }

  static inline std::chrono::nanoseconds SecondsToNanos(const int64_t& secs) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds(secs));
  }

  static inline std::string ToMillisStr(const std::chrono::nanoseconds& nanos) {
    return std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(nanos).count());
  }

  static inline int64_t GetNowNanos() {
    auto duration = std::chrono::high_resolution_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
  }

  static inline int64_t ElapsedMillis(const int64_t& start_ns) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::nanoseconds(GetNowNanos() - start_ns))
        .count();
  }

  static inline std::string ElapsedMillisStr(const int64_t& start_ns) {
    return std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::nanoseconds(GetNowNanos() - start_ns))
                              .count());
  }
};
} /* namespace hbase */

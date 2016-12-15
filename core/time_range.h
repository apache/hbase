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

namespace hbase {
class TimeRange {
 public:
  /**
   * @brief  Default constructor. Represents interval [0, Long.MAX_VALUE)
   * (allTime)
   */
  TimeRange();
  TimeRange(const TimeRange &tr);
  TimeRange &operator=(const TimeRange &tr);
  /**
   * @brief Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  TimeRange(long min_timestamp);
  /**
   * @brief Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws std::runtime_error if min_timestamp < 0 or max_timestamp < 0 or
   * max_timestamp < min_timestamp
   */
  TimeRange(long min_timestamp, long max_timestamp);
  long MinTimeStamp() const;
  long MaxTimeStamp() const;
  bool IsAllTime() const;
  ~TimeRange();

 private:
  long min_timestamp_;
  long max_timestamp_;
  bool all_time_;
};
}

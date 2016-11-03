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

#include "core/time_range.h"
#include <limits>
#include <stdexcept>
#include <string>

namespace hbase {

TimeRange::TimeRange()
    : min_timestamp_(0L),
      max_timestamp_(std::numeric_limits<long>::max()),
      all_time_(true) {

}

TimeRange::TimeRange(const TimeRange &tr) {
  this->all_time_ = tr.all_time_;
  this->max_timestamp_ = tr.max_timestamp_;
  this->min_timestamp_ = tr.min_timestamp_;
}

TimeRange &TimeRange::operator =(const TimeRange &tr) {
  this->all_time_ = tr.all_time_;
  this->max_timestamp_ = tr.max_timestamp_;
  this->min_timestamp_ = tr.min_timestamp_;
  return *this;
}

TimeRange::~TimeRange() {

}

TimeRange::TimeRange(long min_timestamp) {
  this->min_timestamp_ = min_timestamp;
  this->max_timestamp_ = std::numeric_limits<long>::max();
  this->all_time_ = false;
}

TimeRange::TimeRange(long min_timestamp, long max_timestamp) {
  if (min_timestamp < 0 || max_timestamp < 0) {
    throw std::runtime_error(
        "Timestamp cannot be negative. min_timestamp: "
            + std::to_string(min_timestamp) + ", max_timestamp:"
            + std::to_string(max_timestamp));
  }
  if (max_timestamp < min_timestamp) {
    throw std::runtime_error(
        "max_timestamp [" + std::to_string(max_timestamp)
            + "] should be greater than min_timestamp ["
            + std::to_string(min_timestamp) + "]");
  }

  this->min_timestamp_ = min_timestamp;
  this->max_timestamp_ = max_timestamp;
  this->all_time_ = false;
}

long TimeRange::MinTimeStamp() const {
  return this->min_timestamp_;

}

long TimeRange::MaxTimeStamp() const {
  return this->max_timestamp_;
}

bool TimeRange::IsAllTime() const {
  return this->all_time_;
}
}

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

#include <glog/logging.h>
#include <gtest/gtest.h>

using namespace hbase;

TEST(TimeRange, DefaultObject) {
  TimeRange *timerange_def = nullptr;
  ASSERT_NO_THROW(timerange_def = new TimeRange());

  EXPECT_EQ(0, timerange_def->MinTimeStamp());
  EXPECT_EQ(std::numeric_limits<long>::max(), timerange_def->MaxTimeStamp());
  EXPECT_NE(1000, timerange_def->MinTimeStamp());
  EXPECT_NE(2000, timerange_def->MaxTimeStamp());
  delete timerange_def;
  timerange_def = nullptr;
}

TEST(TimeRange, Exception) {
  // Negative Min TS
  ASSERT_THROW(TimeRange(-1000, 2000), std::runtime_error);

  // Negative Max TS
  ASSERT_THROW(TimeRange(1000, -2000), std::runtime_error);

  // Min TS > Max TS
  ASSERT_THROW(TimeRange(10000, 2000), std::runtime_error);
}

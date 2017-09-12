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

#include <algorithm>
#include <climits>
#include <cstdlib>
#include <memory>
#include <vector>
#include "hbase/utils/time-util.h"

namespace hbase {
class ConnectionUtils {
 public:
  static int Retries2Attempts(const int& retries) {
    return std::max(1, retries == INT_MAX ? INT_MAX : retries + 1);
  }

  /* Add a delta to avoid timeout immediately after a retry sleeping. */
  static const uint64_t kSleepDeltaNs = 1000000;

  static const std::vector<uint32_t> kRetryBackoff;
  /**
   * Calculate pause time. Built on {@link kRetryBackoff}.
   * @param pause time to pause
   * @param tries amount of tries
   * @return How long to wait after <code>tries</code> retries
   */
  static int64_t GetPauseTime(const int64_t& pause, const int32_t& tries) {
    int32_t ntries = tries;
    if (static_cast<size_t>(ntries) >= kRetryBackoff.size()) {
      ntries = kRetryBackoff.size() - 1;
    }
    if (ntries < 0) {
      ntries = 0;
    }

    int64_t normal_pause = pause * kRetryBackoff[ntries];
    // 1% possible jitter
    float r = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
    int64_t jitter = (int64_t)(normal_pause * r * 0.01f);
    return normal_pause + jitter;
  }
};
} /* namespace hbase */

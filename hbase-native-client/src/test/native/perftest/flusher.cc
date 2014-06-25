/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#line 19 "flusher.cc" // ensures short filename in logs.

#include <inttypes.h>
#include <unistd.h>

#include "flusher.h"

#include "common_utils.h"

namespace hbase {
namespace test {

void*
Flusher::Run() {
  HBASE_LOG_INFO("Starting Flush thread (0x%08x).", Id());
  uint64_t rpcsSentLast = 0;
  while (Running()) {
    usleep(20000); // sleep for 20 milliseconds
    uint64_t rpcsSent = statKeeper_->GetBufferableOpsCount();
    if ((rpcsSent - rpcsSentLast) > flushBatchSize_) {
      uint64_t startTime = currentTimeMicroSeconds();
      HBASE_LOG_DEBUG("Flushing after %" PRIu64 " rpcs.", rpcsSent);
      flush_client_and_wait(client_);
      uint64_t endTime = currentTimeMicroSeconds();
      rpcsSentLast = rpcsSent;
      statKeeper_->UpdateStats(1,
          (endTime - startTime), StatKeeper::OP_FLUSH, false);
    }
  }
  HBASE_LOG_INFO("Stopping Flush thread (0x%08x) after %" PRIu64 " rpcs.",
      Id(), statKeeper_->GetBufferableOpsCount());
  return NULL;
}

} /* namespace test */
} /* namespace hbase */


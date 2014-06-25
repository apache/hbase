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
#ifndef HBASE_TESTS_STATS_H_
#define HBASE_TESTS_STATS_H_

#include <pthread.h>
#include <stdint.h>
#include <string.h>

#include "test_types.h"

namespace hbase {
namespace test {

class StatKeeper : public TaskRunner {

public:
  typedef enum {
    OP_PUT = 0,
    OP_GET,
    OP_FLUSH,
    OP_LAST
  } OpType;

  struct OperationType {
    OperationType(const char *name);
    ~OperationType();
    const char *name_;
    volatile uint64_t numOps_;
    volatile uint64_t success_;
    volatile uint64_t failure_;
    volatile uint64_t opsStartTime_;
    volatile uint64_t opsEndTime_;
    volatile uint64_t cumLatencyOps_;
    volatile int64_t minLatencyOps_;
    volatile int64_t maxLatencyOps_;
    pthread_mutex_t mutexes_;
  };

  StatKeeper();

  ~StatKeeper();

  void *Run();

  void UpdateStats(uint32_t numOps, uint32_t elapsed, OpType opType, bool isBufferable);

  void RpcComplete(int32_t err, OpType opType);

  volatile uint64_t GetBufferableOpsCount() { return bufferableOpsCount_; }

  void PrintSummary();

  static const char* GetOpName(OpType type) {
    if (type < 0 || type >= OP_LAST) {
      return NULL;
    }
    return OP_TYPE_NAMES[type];
  }

protected:
  volatile uint64_t bufferableOpsCount_;

  OperationType **op_;

  static const char *OP_TYPE_NAMES[];
};

} /* namespace test */
} /* namespace hbase */

#endif /* HBASE_TESTS_STATS_H_ */

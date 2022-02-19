/**

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
 */

package org.apache.hadoop.hbase.ipc;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Queue balancer that just randomly selects a queue in the range [0, num queues).
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RandomQueueBalancer implements QueueBalancer {
  private final int queueSize;
  private final List<BlockingQueue<CallRunner>> queues;

  public RandomQueueBalancer(Configuration conf, String executorName, List<BlockingQueue<CallRunner>> queues) {
    this.queueSize = queues.size();
    this.queues = queues;
  }

  @Override
  public int getNextQueue(CallRunner callRunner) {
    return ThreadLocalRandom.current().nextInt(queueSize);
  }

  /**
   * Exposed for use in tests
   */
  List<BlockingQueue<CallRunner>> getQueues() {
    return queues;
  }
}

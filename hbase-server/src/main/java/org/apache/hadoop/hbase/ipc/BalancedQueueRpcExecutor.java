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

import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * An {@link RpcExecutor} that will balance requests evenly across all its queues, but still remains
 * efficient with a single queue via an inlinable queue balancing mechanism. Defaults to FIFO but
 * you can pass an alternate queue class to use.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class BalancedQueueRpcExecutor extends RpcExecutor {

  private final QueueBalancer balancer;

  public BalancedQueueRpcExecutor(final String name, final int handlerCount,
      final int maxQueueLength, final PriorityFunction priority, final Configuration conf,
      final Abortable abortable) {
    this(name, handlerCount, conf.get(CALL_QUEUE_TYPE_CONF_KEY, CALL_QUEUE_TYPE_CONF_DEFAULT),
        maxQueueLength, priority, conf, abortable);
  }

  public BalancedQueueRpcExecutor(final String name, final int handlerCount,
      final String callQueueType, final int maxQueueLength, final PriorityFunction priority,
      final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, callQueueType, maxQueueLength, priority, conf, abortable);
    this.balancer = getBalancer(this.numCallQueues);
    initializeQueues(this.numCallQueues);
  }

  @Override
  public boolean dispatch(final CallRunner callTask) throws InterruptedException {
    int queueIndex = balancer.getNextQueue();
    BlockingQueue<CallRunner> queue = queues.get(queueIndex);
    // that means we can overflow by at most <num reader> size (5), that's ok
    if (queue.size() >= currentQueueLimit) {
      return false;
    }
    return queue.offer(callTask);
  }
}

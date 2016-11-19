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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * An {@link RpcExecutor} that will balance requests evenly across all its queues, but still remains
 * efficient with a single queue via an inlinable queue balancing mechanism. Defaults to FIFO but
 * you can pass an alternate queue class to use.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class BalancedQueueRpcExecutor extends RpcExecutor {
  private static final Log LOG = LogFactory.getLog(BalancedQueueRpcExecutor.class);

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

  @Deprecated
  public BalancedQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final int maxQueueLength) {
    this(name, handlerCount, numQueues, maxQueueLength, null, null);
  }

  @Deprecated
  public BalancedQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final int maxQueueLength, final Configuration conf, final Abortable abortable) {
    this(name, handlerCount, numQueues, conf, abortable, LinkedBlockingQueue.class, maxQueueLength);
  }

  @Deprecated
  public BalancedQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final Class<? extends BlockingQueue> queueClass, Object... initargs) {
    this(name, handlerCount, numQueues, null, null,  queueClass, initargs);
  }

  @Deprecated
  public BalancedQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final Configuration conf, final Abortable abortable,
      final Class<? extends BlockingQueue> queueClass, Object... initargs) {
    super(name, Math.max(handlerCount, numQueues), numQueues, conf, abortable);
    this.balancer = getBalancer(numQueues);
    initializeQueues(numQueues, queueClass, initargs);
    LOG.debug(name + " queues=" + numQueues + " handlerCount=" + handlerCount);
  }

  @Deprecated
  protected void initializeQueues(final int numQueues,
      final Class<? extends BlockingQueue> queueClass, Object... initargs) {
    if (initargs.length > 0) {
      currentQueueLimit = (int) initargs[0];
      initargs[0] = Math.max((int) initargs[0], DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT);
    }
    for (int i = 0; i < numQueues; ++i) {
      queues.add((BlockingQueue<CallRunner>) ReflectionUtils.newInstance(queueClass, initargs));
    }
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
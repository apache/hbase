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

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import com.google.common.collect.Lists;

/**
 * RPC Executor that dispatch the requests on multiple queues.
 * Each handler has its own queue and there is no stealing.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MultipleQueueRpcExecutor extends RpcExecutor {
  protected final List<BlockingQueue<CallRunner>> queues;
  protected final Random balancer = new Random();

  public MultipleQueueRpcExecutor(final String name, final int handlerCount,
      final int numQueues, final int maxQueueLength) {
    this(name, handlerCount, numQueues, LinkedBlockingQueue.class, maxQueueLength);
  }

  public MultipleQueueRpcExecutor(final String name, final int handlerCount,
      final int numQueues,
      final Class<? extends BlockingQueue> queueClass, Object... initargs) {
    super(name, Math.max(handlerCount, numQueues));
    queues = new ArrayList<BlockingQueue<CallRunner>>(numQueues);
    initializeQueues(numQueues, queueClass, initargs);
  }

  protected void initializeQueues(final int numQueues,
      final Class<? extends BlockingQueue> queueClass, Object... initargs) {
    for (int i = 0; i < numQueues; ++i) {
      queues.add((BlockingQueue<CallRunner>)
        ReflectionUtils.newInstance(queueClass, initargs));
    }
  }

  @Override
  public void dispatch(final CallRunner callTask) throws InterruptedException {
    int queueIndex = balancer.nextInt(queues.size());
    queues.get(queueIndex).put(callTask);
  }

  @Override
  public int getQueueLength() {
    int length = 0;
    for (final BlockingQueue<CallRunner> queue: queues) {
      length += queue.size();
    }
    return length;
  }

  @Override
  protected List<BlockingQueue<CallRunner>> getQueues() {
    return queues;
  }
}

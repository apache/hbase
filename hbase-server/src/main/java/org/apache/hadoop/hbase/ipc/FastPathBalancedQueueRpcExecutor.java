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
 */
package org.apache.hadoop.hbase.ipc;

import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Balanced queue executor with a fastpath. Because this is FIFO, it has no respect for
 * ordering so a fast path skipping the queuing of Calls if an Handler is available, is possible.
 * Just pass the Call direct to waiting Handler thread. Try to keep the hot Handlers bubbling
 * rather than let them go cold and lose context. Idea taken from Apace Kudu (incubating). See
 * https://gerrit.cloudera.org/#/c/2938/7/src/kudu/rpc/service_queue.h
 */
@InterfaceAudience.Private
public class FastPathBalancedQueueRpcExecutor extends BalancedQueueRpcExecutor {
  // Depends on default behavior of BalancedQueueRpcExecutor being FIFO!

  /*
   * Stack of Handlers waiting for work.
   */
  private final Deque<FastPathRpcHandler> fastPathHandlerStack = new ConcurrentLinkedDeque<>();

  public FastPathBalancedQueueRpcExecutor(final String name, final int handlerCount,
      final int maxQueueLength, final PriorityFunction priority, final Configuration conf,
      final Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);
  }

  public FastPathBalancedQueueRpcExecutor(final String name, final int handlerCount,
      final String callQueueType, final int maxQueueLength, final PriorityFunction priority,
      final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, callQueueType, maxQueueLength, priority, conf, abortable);
  }

  @Override
  protected RpcHandler getHandler(final String name, final double handlerFailureThreshhold,
      final int handlerCount, final BlockingQueue<CallRunner> q,
      final AtomicInteger activeHandlerCount, final AtomicInteger failedHandlerCount,
      final Abortable abortable) {
    return new FastPathRpcHandler(name, handlerFailureThreshhold, handlerCount, q,
      activeHandlerCount, failedHandlerCount, abortable, fastPathHandlerStack);
  }

  @Override
  public boolean dispatch(CallRunner callTask) {
    //FastPathHandlers don't check queue limits, so if we're completely shut down
    //we have to prevent ourselves from using the handler in the first place
    if (currentQueueLimit == 0){
      return false;
    }
    FastPathRpcHandler handler = popReadyHandler();
    return handler != null? handler.loadCallRunner(callTask): super.dispatch(callTask);
  }

  /**
   * @return Pop a Handler instance if one available ready-to-go or else return null.
   */
  private FastPathRpcHandler popReadyHandler() {
    return this.fastPathHandlerStack.poll();
  }
}

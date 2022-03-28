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
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * RPC Executor that extends {@link RWQueueRpcExecutor} with fast-path feature, used in
 * {@link FastPathBalancedQueueRpcExecutor}.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class FastPathRWQueueRpcExecutor extends RWQueueRpcExecutor {

  private final Deque<FastPathRpcHandler> readHandlerStack = new ConcurrentLinkedDeque<>();
  private final Deque<FastPathRpcHandler> writeHandlerStack = new ConcurrentLinkedDeque<>();
  private final Deque<FastPathRpcHandler> scanHandlerStack = new ConcurrentLinkedDeque<>();

  public FastPathRWQueueRpcExecutor(String name, int handlerCount, int maxQueueLength,
      PriorityFunction priority, Configuration conf, Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);
  }

  @Override
  protected RpcHandler getHandler(final String name, final double handlerFailureThreshhold,
      final int handlerCount, final BlockingQueue<CallRunner> q,
      final AtomicInteger activeHandlerCount, final AtomicInteger failedHandlerCount,
      final Abortable abortable) {
    Deque<FastPathRpcHandler> handlerStack = name.contains("read") ? readHandlerStack :
      name.contains("write") ? writeHandlerStack : scanHandlerStack;
    return new FastPathRpcHandler(name, handlerFailureThreshhold, handlerCount, q,
      activeHandlerCount, failedHandlerCount, abortable, handlerStack);
  }

  @Override
  public boolean dispatch(final CallRunner callTask) {
    RpcCall call = callTask.getRpcCall();
    boolean shouldDispatchToWriteQueue = isWriteRequest(call.getHeader(), call.getParam());
    boolean shouldDispatchToScanQueue = shouldDispatchToScanQueue(callTask);
    FastPathRpcHandler handler = shouldDispatchToWriteQueue ? writeHandlerStack.poll() :
      shouldDispatchToScanQueue ? scanHandlerStack.poll() : readHandlerStack.poll();
    return handler != null ? handler.loadCallRunner(callTask) :
      dispatchTo(shouldDispatchToWriteQueue, shouldDispatchToScanQueue, callTask);
  }
}

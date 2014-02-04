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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * A scheduler that maintains isolated handler pools for general, high-priority and replication
 * requests.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SimpleRpcScheduler implements RpcScheduler {

  private int port;
  private final int handlerCount;
  private final int priorityHandlerCount;
  private final int replicationHandlerCount;
  private final PriorityFunction priority;
  final BlockingQueue<CallRunner> callQueue;
  final BlockingQueue<CallRunner> priorityCallQueue;
  final BlockingQueue<CallRunner> replicationQueue;
  private volatile boolean running = false;
  private final List<Thread> handlers = Lists.newArrayList();

  /** What level a high priority call is at. */
  private final int highPriorityLevel;

  /**
   * @param conf
   * @param handlerCount the number of handler threads that will be used to process calls
   * @param priorityHandlerCount How many threads for priority handling.
   * @param replicationHandlerCount How many threads for replication handling.
   * @param highPriorityLevel
   * @param priority Function to extract request priority.
   */
  public SimpleRpcScheduler(
      Configuration conf,
      int handlerCount,
      int priorityHandlerCount,
      int replicationHandlerCount,
      PriorityFunction priority,
      int highPriorityLevel) {
    int maxQueueLength = conf.getInt("ipc.server.max.callqueue.length",
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    this.handlerCount = handlerCount;
    this.priorityHandlerCount = priorityHandlerCount;
    this.replicationHandlerCount = replicationHandlerCount;
    this.priority = priority;
    this.highPriorityLevel = highPriorityLevel;
    this.callQueue = new LinkedBlockingQueue<CallRunner>(maxQueueLength);
    this.priorityCallQueue = priorityHandlerCount > 0
        ? new LinkedBlockingQueue<CallRunner>(maxQueueLength)
        : null;
    this.replicationQueue = replicationHandlerCount > 0
        ? new LinkedBlockingQueue<CallRunner>(maxQueueLength)
        : null;
  }

  @Override
  public void init(Context context) {
    this.port = context.getListenerAddress().getPort();
  }

  @Override
  public void start() {
    running = true;
    startHandlers(handlerCount, callQueue, null);
    if (priorityCallQueue != null) {
      startHandlers(priorityHandlerCount, priorityCallQueue, "Priority.");
    }
    if (replicationQueue != null) {
      startHandlers(replicationHandlerCount, replicationQueue, "Replication.");
    }
  }

  private void startHandlers(
      int handlerCount,
      final BlockingQueue<CallRunner> callQueue,
      String threadNamePrefix) {
    for (int i = 0; i < handlerCount; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          consumerLoop(callQueue);
        }
      });
      t.setDaemon(true);
      t.setName(Strings.nullToEmpty(threadNamePrefix) + "RpcServer.handler=" + i + ",port=" + port);
      t.start();
      handlers.add(t);
    }
  }

  @Override
  public void stop() {
    running = false;
    for (Thread handler : handlers) {
      handler.interrupt();
    }
  }

  @Override
  public void dispatch(CallRunner callTask) throws InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int level = priority.getPriority(call.header, call.param);
    if (priorityCallQueue != null && level > highPriorityLevel) {
      priorityCallQueue.put(callTask);
    } else if (replicationQueue != null && level == HConstants.REPLICATION_QOS) {
      replicationQueue.put(callTask);
    } else {
      callQueue.put(callTask); // queue the call; maybe blocked here
    }
  }

  @Override
  public int getGeneralQueueLength() {
    return callQueue.size();
  }

  @Override
  public int getPriorityQueueLength() {
    return priorityCallQueue == null ? 0 : priorityCallQueue.size();
  }

  @Override
  public int getReplicationQueueLength() {
    return replicationQueue == null ? 0 : replicationQueue.size();
  }

  private void consumerLoop(BlockingQueue<CallRunner> myQueue) {
    while (running) {
      try {
        CallRunner task = myQueue.take();
        task.run();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}


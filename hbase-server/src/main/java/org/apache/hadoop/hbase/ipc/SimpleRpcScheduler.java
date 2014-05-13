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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.BoundedPriorityBlockingQueue;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * A scheduler that maintains isolated handler pools for general,
 * high-priority, and replication requests.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SimpleRpcScheduler implements RpcScheduler {
  public static final Log LOG = LogFactory.getLog(SimpleRpcScheduler.class);

  /** If set to true, uses a priority queue and deprioritize long-running scans */
  public static final String CALL_QUEUE_TYPE_CONF_KEY = "ipc.server.callqueue.type";
  public static final String CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE = "deadline";
  public static final String CALL_QUEUE_TYPE_FIFO_CONF_VALUE = "fifo";

  /** max delay in msec used to bound the deprioritized requests */
  public static final String QUEUE_MAX_CALL_DELAY_CONF_KEY = "ipc.server.queue.max.call.delay";

  /**
   * Comparator used by the "normal callQueue" if DEADLINE_CALL_QUEUE_CONF_KEY is set to true.
   * It uses the calculated "deadline" e.g. to deprioritize long-running job
   *
   * If multiple requests have the same deadline BoundedPriorityBlockingQueue will order them in
   * FIFO (first-in-first-out) manner.
   */
  private static class CallPriorityComparator implements Comparator<CallRunner> {
    private final static int DEFAULT_MAX_CALL_DELAY = 5000;

    private final PriorityFunction priority;
    private final int maxDelay;

    public CallPriorityComparator(final Configuration conf, final PriorityFunction priority) {
      this.priority = priority;
      this.maxDelay = conf.getInt(QUEUE_MAX_CALL_DELAY_CONF_KEY, DEFAULT_MAX_CALL_DELAY);
    }

    @Override
    public int compare(CallRunner a, CallRunner b) {
      RpcServer.Call callA = a.getCall();
      RpcServer.Call callB = b.getCall();
      long deadlineA = priority.getDeadline(callA.getHeader(), callA.param);
      long deadlineB = priority.getDeadline(callB.getHeader(), callB.param);
      deadlineA = callA.timestamp + Math.min(deadlineA, maxDelay);
      deadlineB = callB.timestamp + Math.min(deadlineB, maxDelay);
      return (int)(deadlineA - deadlineB);
    }
  }

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
  private AtomicInteger activeHandlerCount = new AtomicInteger(0);
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

    String callQueueType = conf.get(CALL_QUEUE_TYPE_CONF_KEY, CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE);
    LOG.debug("Using " + callQueueType + " as user call queue");
    if (callQueueType.equals(CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE)) {
      this.callQueue = new BoundedPriorityBlockingQueue<CallRunner>(maxQueueLength,
          new CallPriorityComparator(conf, this.priority));
    } else {
      this.callQueue = new LinkedBlockingQueue<CallRunner>(maxQueueLength);
    }
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
    int level = priority.getPriority(call.getHeader(), call.param);
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

  @Override
  public int getActiveRpcHandlerCount() {
    return activeHandlerCount.get();
  }

  private void consumerLoop(BlockingQueue<CallRunner> myQueue) {
    boolean interrupted = false;
    try {
      while (running) {
        try {
          CallRunner task = myQueue.take();
          try {
            activeHandlerCount.incrementAndGet();
            task.run();
          } finally {
            activeHandlerCount.decrementAndGet();
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}


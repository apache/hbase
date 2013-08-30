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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
  final BlockingQueue<RpcServer.CallRunner> callQueue;
  final BlockingQueue<RpcServer.CallRunner> priorityCallQueue;
  final BlockingQueue<RpcServer.CallRunner> replicationQueue;
  private volatile boolean running = false;
  private final List<Thread> handlers = Lists.newArrayList();
  private final Function<Pair<RPCProtos.RequestHeader, Message>, Integer> qosFunction;

  /** What level a high priority call is at. */
  private final int highPriorityLevel;

  /**
   * @param conf
   * @param handlerCount the number of handler threads that will be used to process calls
   * @param priorityHandlerCount How many threads for priority handling.
   * @param replicationHandlerCount How many threads for replication handling.
   * @param qosFunction a function that maps requests to priorities
   * @param highPriorityLevel
   */
  public SimpleRpcScheduler(
      Configuration conf,
      int handlerCount,
      int priorityHandlerCount,
      int replicationHandlerCount,
      Function<Pair<RPCProtos.RequestHeader, Message>, Integer> qosFunction,
      int highPriorityLevel) {
    int maxQueueLength = conf.getInt("ipc.server.max.callqueue.length",
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    this.handlerCount = handlerCount;
    this.priorityHandlerCount = priorityHandlerCount;
    this.replicationHandlerCount = replicationHandlerCount;
    this.qosFunction = qosFunction;
    this.highPriorityLevel = highPriorityLevel;
    this.callQueue = new LinkedBlockingQueue<RpcServer.CallRunner>(maxQueueLength);
    this.priorityCallQueue = priorityHandlerCount > 0
        ? new LinkedBlockingQueue<RpcServer.CallRunner>(maxQueueLength)
        : null;
    this.replicationQueue = replicationHandlerCount > 0
        ? new LinkedBlockingQueue<RpcServer.CallRunner>(maxQueueLength)
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
      final BlockingQueue<RpcServer.CallRunner> callQueue,
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
  public void dispatch(RpcServer.CallRunner callTask) throws InterruptedException {
    RpcServer.Call call = callTask.getCall();
    Pair<RPCProtos.RequestHeader, Message> headerAndParam =
        new Pair<RPCProtos.RequestHeader, Message>(call.header, call.param);
    int level = getQosLevel(headerAndParam);
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

  private void consumerLoop(BlockingQueue<RpcServer.CallRunner> myQueue) {
    while (running) {
      try {
        RpcServer.CallRunner task = myQueue.take();
        task.run();
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
    }
  }

  private int getQosLevel(Pair<RPCProtos.RequestHeader, Message> headerAndParam) {
    if (qosFunction == null) return 0;
    Integer res = qosFunction.apply(headerAndParam);
    return res == null? 0: res;
  }
}


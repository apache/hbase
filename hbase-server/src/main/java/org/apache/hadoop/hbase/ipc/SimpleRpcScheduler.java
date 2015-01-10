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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A scheduler that maintains isolated handler pools for general, high-priority and replication
 * requests.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class SimpleRpcScheduler extends RpcScheduler {
  public static final Log LOG = LogFactory.getLog(SimpleRpcScheduler.class);

  public static final String CALL_QUEUE_READ_SHARE_CONF_KEY =
    "hbase.ipc.server.callqueue.read.share";
  public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY =
    "hbase.ipc.server.callqueue.handler.factor";
  public static final String CALL_QUEUE_MAX_LENGTH_CONF_KEY =
    "hbase.ipc.server.max.callqueue.length";

  private int port;
  private final PriorityFunction priority;
  private final RpcExecutor callExecutor;
  private final RpcExecutor priorityExecutor;
  private final RpcExecutor replicationExecutor;

  /** What level a high priority call is at. */
  private final int highPriorityLevel;
  
  private final Abortable abortable;

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
      Abortable abortable,
      int highPriorityLevel) {
    int maxQueueLength = conf.getInt(CALL_QUEUE_MAX_LENGTH_CONF_KEY,
      conf.getInt("ipc.server.max.callqueue.length",
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER));
    this.priority = priority;
    this.highPriorityLevel = highPriorityLevel;
    this.abortable = abortable;

    float callqReadShare = conf.getFloat(CALL_QUEUE_READ_SHARE_CONF_KEY,
      conf.getFloat("ipc.server.callqueue.read.share", 0));

    float callQueuesHandlersFactor = conf.getFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY,
      conf.getFloat("ipc.server.callqueue.handler.factor", 0));
    int numCallQueues = Math.max(1, (int)Math.round(handlerCount * callQueuesHandlersFactor));

    LOG.info("Using default user call queue, count=" + numCallQueues);

    if (numCallQueues > 1 && callqReadShare > 0) {
      // multiple read/write queues
      callExecutor = new RWQueueRpcExecutor("RW.Default", handlerCount, numCallQueues,
        callqReadShare, maxQueueLength, conf, abortable);
    } else {
      // multiple queues
      callExecutor = new BalancedQueueRpcExecutor("B.Default", handlerCount,
        numCallQueues, maxQueueLength, conf, abortable);
    }

    this.priorityExecutor =
        priorityHandlerCount > 0 ? new BalancedQueueRpcExecutor("Priority", priorityHandlerCount,
          1, maxQueueLength, conf, abortable) : null;
    this.replicationExecutor =
       replicationHandlerCount > 0 ? new BalancedQueueRpcExecutor("Replication",
         replicationHandlerCount, 1, maxQueueLength, conf, abortable) : null;
  }

  @Override
  public void init(Context context) {
    this.port = context.getListenerAddress().getPort();
  }

    @Override
  public void start() {
    callExecutor.start(port);
    if (priorityExecutor != null) priorityExecutor.start(port);
    if (replicationExecutor != null) replicationExecutor.start(port);
  }

  @Override
  public void stop() {
    callExecutor.stop();
    if (priorityExecutor != null) priorityExecutor.stop();
    if (replicationExecutor != null) replicationExecutor.stop();
  }

  @Override
  public void dispatch(CallRunner callTask) throws InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int level = priority.getPriority(call.getHeader(), call.param);
    if (priorityExecutor != null && level > highPriorityLevel) {
      priorityExecutor.dispatch(callTask);
    } else if (replicationExecutor != null && level == HConstants.REPLICATION_QOS) {
      replicationExecutor.dispatch(callTask);
    } else {
      callExecutor.dispatch(callTask);
    }
  }

  @Override
  public int getGeneralQueueLength() {
    return callExecutor.getQueueLength();
  }

  @Override
  public int getPriorityQueueLength() {
    return priorityExecutor == null ? 0 : priorityExecutor.getQueueLength();
  }

  @Override
  public int getReplicationQueueLength() {
    return replicationExecutor == null ? 0 : replicationExecutor.getQueueLength();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return callExecutor.getActiveHandlerCount() +
           (priorityExecutor == null ? 0 : priorityExecutor.getActiveHandlerCount()) +
           (replicationExecutor == null ? 0 : replicationExecutor.getActiveHandlerCount());
  }
}


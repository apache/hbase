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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.BoundedPriorityBlockingQueue;

/**
 * A scheduler that maintains isolated handler pools for general,
 * high-priority, and replication requests.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class SimpleRpcScheduler extends RpcScheduler {
  public static final Log LOG = LogFactory.getLog(SimpleRpcScheduler.class);

  public static final String CALL_QUEUE_READ_SHARE_CONF_KEY =
      "hbase.ipc.server.callqueue.read.ratio";
  public static final String CALL_QUEUE_SCAN_SHARE_CONF_KEY =
      "hbase.ipc.server.callqueue.scan.ratio";
  public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY =
      "hbase.ipc.server.callqueue.handler.factor";

  /** If set to 'deadline', uses a priority queue and deprioritize long-running scans */
  public static final String CALL_QUEUE_TYPE_CONF_KEY = "hbase.ipc.server.callqueue.type";
  public static final String CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE = "deadline";
  public static final String CALL_QUEUE_TYPE_FIFO_CONF_VALUE = "fifo";

  /** max delay in msec used to bound the deprioritized requests */
  public static final String QUEUE_MAX_CALL_DELAY_CONF_KEY
      = "hbase.ipc.server.queue.max.call.delay";

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
  private final PriorityFunction priority;
  private final RpcExecutor callExecutor;
  private final RpcExecutor priorityExecutor;
  private final RpcExecutor replicationExecutor;

  /** What level a high priority call is at. */
  private final int highPriorityLevel;

  private Abortable abortable = null;

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
      Abortable server,
      int highPriorityLevel) {
    int maxQueueLength = conf.getInt("hbase.ipc.server.max.callqueue.length",
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    this.priority = priority;
    this.highPriorityLevel = highPriorityLevel;
    this.abortable = server;

    String callQueueType = conf.get(CALL_QUEUE_TYPE_CONF_KEY, CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE);
    float callqReadShare = conf.getFloat(CALL_QUEUE_READ_SHARE_CONF_KEY, 0);
    float callqScanShare = conf.getFloat(CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0);

    float callQueuesHandlersFactor = conf.getFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 0);
    int numCallQueues = Math.max(1, (int)Math.round(handlerCount * callQueuesHandlersFactor));

    LOG.info("Using " + callQueueType + " as user call queue, count=" + numCallQueues);

    if (numCallQueues > 1 && callqReadShare > 0) {
      // multiple read/write queues
      if (callQueueType.equals(CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE)) {
        CallPriorityComparator callPriority = new CallPriorityComparator(conf, this.priority);
        callExecutor = new RWQueueRpcExecutor("RW.default", handlerCount, numCallQueues,
            callqReadShare, callqScanShare, maxQueueLength, conf, abortable,
            BoundedPriorityBlockingQueue.class, callPriority);
      } else {
        callExecutor = new RWQueueRpcExecutor("RW.default", handlerCount, numCallQueues,
          callqReadShare, callqScanShare, maxQueueLength, conf, abortable);
      }
    } else {
      // multiple queues
      if (callQueueType.equals(CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE)) {
        CallPriorityComparator callPriority = new CallPriorityComparator(conf, this.priority);
        callExecutor = new BalancedQueueRpcExecutor("B.default", handlerCount, numCallQueues,
          conf, abortable, BoundedPriorityBlockingQueue.class, maxQueueLength, callPriority);
      } else {
        callExecutor = new BalancedQueueRpcExecutor("B.default", handlerCount,
            numCallQueues, maxQueueLength, conf, abortable);
      }
    }

   this.priorityExecutor =
     priorityHandlerCount > 0 ? new BalancedQueueRpcExecutor("Priority", priorityHandlerCount,
       1, maxQueueLength, conf, abortable) : null;
   this.replicationExecutor =
     replicationHandlerCount > 0 ? new BalancedQueueRpcExecutor("Replication",
       replicationHandlerCount, 1, maxQueueLength, conf, abortable) : null;
  }

  public SimpleRpcScheduler(
	      Configuration conf,
	      int handlerCount,
	      int priorityHandlerCount,
	      int replicationHandlerCount,
	      PriorityFunction priority,
	      int highPriorityLevel) {
	  this(conf, handlerCount, priorityHandlerCount, replicationHandlerCount, priority,
	    null, highPriorityLevel);
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


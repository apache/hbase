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
import org.apache.hadoop.hbase.conf.ConfigurationObserver;

/**
 * The default scheduler. Configurable. Maintains isolated handler pools for general ('default'),
 * high-priority ('priority'), and replication ('replication') requests. Default behavior is to
 * balance the requests across handlers. Add configs to enable balancing by read vs writes, etc.
 * See below article for explanation of options.
 * @see <a href="http://blog.cloudera.com/blog/2014/12/new-in-cdh-5-2-improvements-for-running-multiple-workloads-on-a-single-hbase-cluster/">Overview on Request Queuing</a>
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class SimpleRpcScheduler extends RpcScheduler implements ConfigurationObserver {
  private static final Log LOG = LogFactory.getLog(SimpleRpcScheduler.class);

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

    int maxQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH,
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    int maxPriorityQueueLength =
        conf.getInt(RpcScheduler.IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH, maxQueueLength);

    this.priority = priority;
    this.highPriorityLevel = highPriorityLevel;
    this.abortable = server;

    String callQueueType = conf.get(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY,
      RpcExecutor.CALL_QUEUE_TYPE_CONF_DEFAULT);
    float callqReadShare = conf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0);

    if (callqReadShare > 0) {
      // at least 1 read handler and 1 write handler
      callExecutor = new RWQueueRpcExecutor("deafult.RWQ", Math.max(2, handlerCount),
        maxQueueLength, priority, conf, server);
    } else {
      if (RpcExecutor.isFifoQueueType(callQueueType)) {
        callExecutor = new FastPathBalancedQueueRpcExecutor("deafult.FPBQ", handlerCount,
            maxPriorityQueueLength, priority, conf, server);
      } else {
        callExecutor = new BalancedQueueRpcExecutor("deafult.BQ", handlerCount, maxQueueLength,
            priority, conf, server);
      }
    }

    // Create 2 queues to help priorityExecutor be more scalable.
    this.priorityExecutor = priorityHandlerCount > 0 ? new FastPathBalancedQueueRpcExecutor(
        "priority.FPBQ", priorityHandlerCount, RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE,
        maxPriorityQueueLength, priority, conf, abortable) : null;
    this.replicationExecutor = replicationHandlerCount > 0 ? new FastPathBalancedQueueRpcExecutor(
        "replication.FPBQ", replicationHandlerCount, RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE,
        maxQueueLength, priority, conf, abortable) : null;
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

  /**
   * Resize call queues;
   * @param conf new configuration
   */
  @Override
  public void onConfigurationChange(Configuration conf) {
    callExecutor.resizeQueues(conf);
    if (priorityExecutor != null) {
      priorityExecutor.resizeQueues(conf);
    }
    if (replicationExecutor != null) {
      replicationExecutor.resizeQueues(conf);
    }

    String callQueueType = conf.get(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY,
      RpcExecutor.CALL_QUEUE_TYPE_CONF_DEFAULT);
    if (RpcExecutor.isCodelQueueType(callQueueType)) {
      callExecutor.onConfigurationChange(conf);
    }
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
  public boolean dispatch(CallRunner callTask) throws InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int level = priority.getPriority(call.getHeader(), call.param, call.getRequestUser());
    if (priorityExecutor != null && level > highPriorityLevel) {
      return priorityExecutor.dispatch(callTask);
    } else if (replicationExecutor != null && level == HConstants.REPLICATION_QOS) {
      return replicationExecutor.dispatch(callTask);
    } else {
      return callExecutor.dispatch(callTask);
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

  @Override
  public long getNumGeneralCallsDropped() {
    return callExecutor.getNumGeneralCallsDropped();
  }

  @Override
  public long getNumLifoModeSwitches() {
    return callExecutor.getNumLifoModeSwitches();
  }

  @Override
  public int getWriteQueueLength() {
    return callExecutor.getWriteQueueLength();
  }

  @Override
  public int getReadQueueLength() {
    return callExecutor.getReadQueueLength();
  }

  @Override
  public int getScanQueueLength() {
    return callExecutor.getScanQueueLength();
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    return callExecutor.getActiveWriteHandlerCount();
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    return callExecutor.getActiveReadHandlerCount();
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    return callExecutor.getActiveScanHandlerCount();
  }
}


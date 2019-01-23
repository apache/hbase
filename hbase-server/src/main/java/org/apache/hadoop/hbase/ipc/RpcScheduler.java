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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An interface for RPC request scheduling algorithm.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public abstract class RpcScheduler {

  public static final String IPC_SERVER_MAX_CALLQUEUE_LENGTH =
      "hbase.ipc.server.max.callqueue.length";
  public static final String IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH =
      "hbase.ipc.server.priority.max.callqueue.length";

  /** Exposes runtime information of a {@code RpcServer} that a {@code RpcScheduler} may need. */
  public static abstract class Context {
    public abstract InetSocketAddress getListenerAddress();
  }

  /**
   * Does some quick initialization. Heavy tasks (e.g. starting threads) should be
   * done in {@link #start()}. This method is called before {@code start}.
   *
   * @param context provides methods to retrieve runtime information from
   */
  public abstract void init(Context context);

  /**
   * Prepares for request serving. An implementation may start some handler threads here.
   */
  public abstract void start();

  /** Stops serving new requests. */
  public abstract void stop();

  /**
   * Dispatches an RPC request asynchronously. An implementation is free to choose to process the
   * request immediately or delay it for later processing.
   *
   * @param task the request to be dispatched
   */
  public abstract boolean dispatch(CallRunner task) throws IOException, InterruptedException;

  /** Get call queue information **/
  public abstract CallQueueInfo getCallQueueInfo();

  /** Retrieves length of the general queue for metrics. */
  public abstract int getGeneralQueueLength();

  /** Retrieves length of the priority queue for metrics. */
  public abstract int getPriorityQueueLength();

  /** Retrieves length of the meta priority queue for metrics. */
  public abstract int getMetaPriorityQueueLength();

  /** Retrieves length of the replication queue for metrics. */
  public abstract int getReplicationQueueLength();

  /** Retrieves the total number of active handler. */
  public abstract int getActiveRpcHandlerCount();

  /** Retrieves the number of active general handler. */
  public abstract int getActiveGeneralRpcHandlerCount();

  /** Retrieves the number of active priority handler. */
  public abstract int getActivePriorityRpcHandlerCount();

  /** Retrieves the number of active meta priority handler. */
  public abstract int getActiveMetaPriorityRpcHandlerCount();

  /** Retrieves the number of active replication handler. */
  public abstract int getActiveReplicationRpcHandlerCount();

  /**
   * If CoDel-based RPC executors are used, retrieves the number of Calls that were dropped
   * from general queue because RPC executor is under high load; returns 0 otherwise.
   */
  public abstract long getNumGeneralCallsDropped();

  /**
   * If CoDel-based RPC executors are used, retrieves the number of Calls that were
   * picked from the tail of the queue (indicating adaptive LIFO mode, when
   * in the period of overloade we serve last requests first); returns 0 otherwise.
   */
  public abstract long getNumLifoModeSwitches();

  /** Retrieves length of the write queue for metrics when use RWQueueRpcExecutor. */
  public abstract int getWriteQueueLength();

  /** Retrieves length of the read queue for metrics when use RWQueueRpcExecutor. */
  public abstract int getReadQueueLength();

  /** Retrieves length of the scan queue for metrics when use RWQueueRpcExecutor. */
  public abstract int getScanQueueLength();

  /** Retrieves the number of active write rpc handler when use RWQueueRpcExecutor. */
  public abstract int getActiveWriteRpcHandlerCount();

  /** Retrieves the number of active write rpc handler when use RWQueueRpcExecutor. */
  public abstract int getActiveReadRpcHandlerCount();

  /** Retrieves the number of active write rpc handler when use RWQueueRpcExecutor. */
  public abstract int getActiveScanRpcHandlerCount();
}

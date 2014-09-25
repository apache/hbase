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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An interface for RPC request scheduling algorithm.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public abstract class RpcScheduler {

  /** Exposes runtime information of a {@code RpcServer} that a {@code RpcScheduler} may need. */
  static abstract class Context {
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
  public abstract void dispatch(CallRunner task) throws IOException, InterruptedException;

  /** Retrieves length of the general queue for metrics. */
  public abstract int getGeneralQueueLength();

  /** Retrieves length of the priority queue for metrics. */
  public abstract int getPriorityQueueLength();

  /** Retrieves length of the replication queue for metrics. */
  public abstract int getReplicationQueueLength();

  /** Retrieves the number of active handler. */
  public abstract int getActiveRpcHandlerCount();
}

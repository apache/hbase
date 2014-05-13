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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An interface for RPC request scheduling algorithm.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface RpcScheduler {

  /** Exposes runtime information of a {@code RpcServer} that a {@code RpcScheduler} may need. */
  interface Context {
    InetSocketAddress getListenerAddress();
  }

  /**
   * Does some quick initialization. Heavy tasks (e.g. starting threads) should be
   * done in {@link #start()}. This method is called before {@code start}.
   *
   * @param context provides methods to retrieve runtime information from
   */
  void init(Context context);

  /**
   * Prepares for request serving. An implementation may start some handler threads here.
   */
  void start();

  /** Stops serving new requests. */
  void stop();

  /**
   * Dispatches an RPC request asynchronously. An implementation is free to choose to process the
   * request immediately or delay it for later processing.
   *
   * @param task the request to be dispatched
   */
  void dispatch(CallRunner task) throws IOException, InterruptedException;

  /** Retrieves length of the general queue for metrics. */
  int getGeneralQueueLength();

  /** Retrieves length of the priority queue for metrics. */
  int getPriorityQueueLength();

  /** Retrieves length of the replication queue for metrics. */
  int getReplicationQueueLength();

  /** Retrieves the number of active handler. */
  int getActiveRpcHandlerCount();
}

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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;

/**
 * Interface for Async Rpc Channels
 */
@InterfaceAudience.Private
public interface AsyncRpcChannel {

  /**
   * Calls method on channel
   * @param method to call
   * @param controller to run call with
   * @param request to send
   * @param responsePrototype to construct response with
   */
  Promise<Message> callMethod(final Descriptors.MethodDescriptor method,
      final PayloadCarryingRpcController controller, final Message request,
      final Message responsePrototype, MetricsConnection.CallStats callStats);

  /**
   * Get the EventLoop on which this channel operated
   * @return EventLoop
   */
  EventExecutor getEventExecutor();

  /**
   * Close connection
   * @param cause of closure.
   */
  void close(Throwable cause);

  /**
   * Check if the connection is alive
   *
   * @return true if alive
   */
  boolean isAlive();

  /**
   * Get the address on which this channel operates
   * @return InetSocketAddress
   */
  InetSocketAddress getAddress();
}

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

import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingRpcChannel;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;

/**
 * Interface for RpcClient implementations so ConnectionManager can handle it.
 */
@InterfaceAudience.Private
public interface RpcClient extends Closeable {
  String FAILED_SERVER_EXPIRY_KEY = "hbase.ipc.client.failed.servers.expiry";
  int FAILED_SERVER_EXPIRY_DEFAULT = 2000;
  String IDLE_TIME = "hbase.ipc.client.connection.minIdleTimeBeforeClose";
  String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY =
      "hbase.ipc.client.fallback-to-simple-auth-allowed";
  boolean IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT = false;
  String SPECIFIC_WRITE_THREAD = "hbase.ipc.client.specificThreadForWriting";
  String DEFAULT_CODEC_CLASS = "hbase.client.default.rpc.codec";

  String SOCKET_TIMEOUT_CONNECT = "hbase.ipc.client.socket.timeout.connect";
  /**
   * How long we wait when we wait for an answer. It's not the operation time, it's the time
   * we wait when we start to receive an answer, when the remote write starts to send the data.
   */
  String SOCKET_TIMEOUT_READ = "hbase.ipc.client.socket.timeout.read";
  String SOCKET_TIMEOUT_WRITE = "hbase.ipc.client.socket.timeout.write";
  int DEFAULT_SOCKET_TIMEOUT_CONNECT = 10000; // 10 seconds
  int DEFAULT_SOCKET_TIMEOUT_READ = 20000; // 20 seconds
  int DEFAULT_SOCKET_TIMEOUT_WRITE = 60000; // 60 seconds

  // Used by the server, for compatibility with old clients.
  // The client in 0.99+ does not ping the server.
  int PING_CALL_ID = -1;

  /**
   * Creates a "channel" that can be used by a blocking protobuf service.  Useful setting up
   * protobuf blocking stubs.
   *
   * @param sn server name describing location of server
   * @param user which is to use the connection
   * @param rpcTimeout default rpc operation timeout
   *
   * @return A blocking rpc channel that goes via this rpc client instance.
   * @throws IOException when channel could not be created
   */
  BlockingRpcChannel createBlockingRpcChannel(ServerName sn, User user, int rpcTimeout)
      throws IOException;

  /**
   * Creates a "channel" that can be used by a protobuf service.  Useful setting up
   * protobuf stubs.
   *
   * @param sn server name describing location of server
   * @param user which is to use the connection
   * @param rpcTimeout default rpc operation timeout
   *
   * @return A rpc channel that goes via this rpc client instance.
   */
  RpcChannel createRpcChannel(final ServerName sn, final User user, int rpcTimeout)
      throws IOException;

  /**
   * Interrupt the connections to the given server. This should be called if the server
   * is known as actually dead. This will not prevent current operation to be retried, and,
   * depending on their own behavior, they may retry on the same server. This can be a feature,
   * for example at startup. In any case, they're likely to get connection refused (if the
   * process died) or no route to host: i.e. their next retries should be faster and with a
   * safe exception.
   * @param sn server location to cancel connections of
   */
  void cancelConnections(ServerName sn);

  /**
   * Stop all threads related to this client.  No further calls may be made
   * using this client.
   */
  @Override
  void close();

  /**
   * @return true when this client uses a {@link org.apache.hadoop.hbase.codec.Codec} and so
   *         supports cell blocks.
   */
  boolean hasCellBlockSupport();
}
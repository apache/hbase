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

import java.io.IOException;
import java.net.SocketAddress;
import javax.net.SocketFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.net.NetUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Does RPC against a cluster. Manages connections per regionserver in the cluster.
 * <p>
 * See HBaseServer
 */
@InterfaceAudience.Private
public class BlockingRpcClient extends AbstractRpcClient<BlockingRpcConnection> {

  protected final SocketFactory socketFactory; // how to create sockets

  /**
   * Used in test only. Construct an IPC client for the cluster {@code clusterId} with the default
   * SocketFactory
   */
  BlockingRpcClient(Configuration conf) {
    this(conf, HConstants.CLUSTER_ID_DEFAULT, null, null);
  }

  /**
   * Construct an IPC client for the cluster {@code clusterId} with the default SocketFactory This
   * method is called with reflection by the RpcClientFactory to create an instance
   * @param conf configuration
   * @param clusterId the cluster id
   * @param localAddr client socket bind address.
   * @param metrics the connection metrics
   */
  public BlockingRpcClient(Configuration conf, String clusterId, SocketAddress localAddr,
      MetricsConnection metrics) {
    super(conf, clusterId, localAddr, metrics);
    this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
  }

  /**
   * Creates a connection. Can be overridden by a subclass for testing.
   * @param remoteId - the ConnectionId to use for the connection creation.
   */
  @Override
  protected BlockingRpcConnection createConnection(ConnectionId remoteId) throws IOException {
    return new BlockingRpcConnection(this, remoteId);
  }

  @Override
  protected void closeInternal() {
  }
}

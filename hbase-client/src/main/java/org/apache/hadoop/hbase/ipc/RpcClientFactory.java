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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import java.net.SocketAddress;

/**
 * Factory to create a {@link org.apache.hadoop.hbase.ipc.RpcClient}
 */
@InterfaceAudience.Private
public final class RpcClientFactory {

  public static final String CUSTOM_RPC_CLIENT_IMPL_CONF_KEY = "hbase.rpc.client.impl";

  /**
   * Private Constructor
   */
  private RpcClientFactory() {
  }

  /**
   * Creates a new RpcClient by the class defined in the configuration or falls back to
   * RpcClientImpl
   * @param conf configuration
   * @param clusterId the cluster id
   * @return newly created RpcClient
   */
  public static RpcClient createClient(Configuration conf, String clusterId) {
    return createClient(conf, clusterId, null);
  }

  /**
   * Creates a new RpcClient by the class defined in the configuration or falls back to
   * RpcClientImpl
   * @param conf configuration
   * @param clusterId the cluster id
   * @param localAddr client socket bind address.
   * @return newly created RpcClient
   */
  public static RpcClient createClient(Configuration conf, String clusterId,
      SocketAddress localAddr) {
    String rpcClientClass =
        conf.get(CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, AsyncRpcClient.class.getName());
    return ReflectionUtils.instantiateWithCustomCtor(
        rpcClientClass,
        new Class[] { Configuration.class, String.class, SocketAddress.class },
        new Object[] { conf, clusterId, localAddr }
    );
  }
}
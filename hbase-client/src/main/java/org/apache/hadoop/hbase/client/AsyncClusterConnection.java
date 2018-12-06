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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The asynchronous connection for internal usage.
 */
@InterfaceAudience.Private
public interface AsyncClusterConnection extends AsyncConnection {

  /**
   * Get the admin service for the given region server.
   */
  AsyncRegionServerAdmin getRegionServerAdmin(ServerName serverName);

  /**
   * Get the nonce generator for this connection.
   */
  NonceGenerator getNonceGenerator();

  /**
   * Get the rpc client we used to communicate with other servers.
   */
  RpcClient getRpcClient();
}

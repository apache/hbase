/*
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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * ZK based rpc throttle storage.
 */
@InterfaceAudience.Private
public class RpcThrottleStorage {
  public static final String RPC_THROTTLE_ZNODE = "zookeeper.znode.quota.rpc.throttle";
  public static final String RPC_THROTTLE_ZNODE_DEFAULT = "rpc-throttle";

  private final ZKWatcher zookeeper;
  private final String rpcThrottleZNode;

  public RpcThrottleStorage(ZKWatcher zookeeper, Configuration conf) {
    this.zookeeper = zookeeper;
    this.rpcThrottleZNode = ZNodePaths.joinZNode(zookeeper.getZNodePaths().baseZNode,
      conf.get(RPC_THROTTLE_ZNODE, RPC_THROTTLE_ZNODE_DEFAULT));
  }

  public boolean isRpcThrottleEnabled() throws IOException {
    try {
      byte[] upData = ZKUtil.getData(zookeeper, rpcThrottleZNode);
      return upData == null || Bytes.toBoolean(upData);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to get rpc throttle", e);
    }
  }

  /**
   * Store the rpc throttle value.
   * @param enable Set to <code>true</code> to enable, <code>false</code> to disable.
   * @throws IOException if an unexpected io exception occurs
   */
  public void switchRpcThrottle(boolean enable) throws IOException {
    try {
      byte[] upData = Bytes.toBytes(enable);
      ZKUtil.createSetData(zookeeper, rpcThrottleZNode, upData);
    } catch (KeeperException e) {
      throw new IOException("Failed to store rpc throttle", e);
    }
  }
}

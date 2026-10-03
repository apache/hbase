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
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.BooleanStateStore;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Store whether rpc throttle is enabled.
 * <p>
 * Notice that, this is stored in master local region so only master can read it. For region
 * servers, we will publish the change through
 * {@link org.apache.hadoop.hbase.master.procedure.SwitchRpcThrottleProcedure} and region servers
 * can also query the flag through rpc request to master.
 */
@InterfaceAudience.Private
public class RpcThrottleStateStore extends BooleanStateStore {

  public static final String RPC_THROTTLE_ZNODE = "zookeeper.znode.quota.rpc.throttle";
  public static final String RPC_THROTTLE_ZNODE_DEFAULT = "rpc-throttle";

  public static final String STATE_NAME = "rpc-throttle";

  public RpcThrottleStateStore(MasterRegion masterRegion, ZKWatcher watcher, String zkPath)
    throws IOException, KeeperException, DeserializationException {
    super(masterRegion, STATE_NAME, watcher,
      ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, zkPath));
  }

  @Override
  protected byte[] toByteArray(boolean on) {
    return Bytes.toBytes(on);
  }

  @Override
  protected boolean parseFrom(byte[] bytes) throws DeserializationException {
    return Bytes.toBoolean(bytes);
  }

}

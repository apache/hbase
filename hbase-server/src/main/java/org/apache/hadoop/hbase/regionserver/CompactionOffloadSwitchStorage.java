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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * ZK based compaction offload switch storage.
 */
@InterfaceAudience.Private
public class CompactionOffloadSwitchStorage {
  private static final String COMPACTION_OFFLOAD_ZNODE = "zookeeper.znode.compaction.offload";
  private static final String COMPACTION_OFFLOAD_ZNODE_DEFAULT = "compaction-offload";

  private final ZKWatcher zookeeper;
  private final String compactionOffloadZnode;

  public CompactionOffloadSwitchStorage(ZKWatcher zookeeper, Configuration conf) {
    this.zookeeper = zookeeper;
    this.compactionOffloadZnode = ZNodePaths.joinZNode(zookeeper.getZNodePaths().baseZNode,
      conf.get(COMPACTION_OFFLOAD_ZNODE, COMPACTION_OFFLOAD_ZNODE_DEFAULT));
  }

  public boolean isCompactionOffloadEnabled() throws IOException {
    try {
      byte[] upData = ZKUtil.getData(zookeeper, compactionOffloadZnode);
      return upData != null && Bytes.toBoolean(upData);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to get compaction offload enable value", e);
    }
  }

  /**
   * Store the compaction offload enable value.
   * @param enable Set to <code>true</code> to enable, <code>false</code> to disable.
   * @throws IOException if an unexpected io exception occurs
   */
  public void switchCompactionOffload(boolean enable) throws IOException {
    try {
      byte[] upData = Bytes.toBytes(enable);
      ZKUtil.createSetData(zookeeper, compactionOffloadZnode, upData);
    } catch (KeeperException e) {
      throw new IOException("Failed to store compaction offload enable value", e);
    }
  }
}

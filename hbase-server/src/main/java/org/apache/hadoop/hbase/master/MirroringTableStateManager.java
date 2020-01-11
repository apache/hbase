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
package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.client.TableState.State;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A subclass of TableStateManager that mirrors change in state out to zookeeper for hbase-1.x
 * clients to pick up; hbase-1.x clients read table state of zookeeper rather than from hbase:meta
 * as hbase-2.x clients do. Set "hbase.mirror.table.state.to.zookeeper" to false to disable
 * mirroring. See in HMaster where we make the choice. The below does zk updates on a best-effort
 * basis only. If we fail updating zk we keep going because only hbase1 clients suffer; we'll just
 * log at WARN level.
 * @deprecated Since 2.0.0. To be removed in 3.0.0.
 */
@Deprecated
@InterfaceAudience.Private
public class MirroringTableStateManager extends TableStateManager {
  private static final Logger LOG = LoggerFactory.getLogger(MirroringTableStateManager.class);

  /**
   * Set this key to true in Configuration to enable mirroring of table state out to zookeeper so
   * hbase-1.x clients can pick-up table state.
   */
  static final String MIRROR_TABLE_STATE_TO_ZK_KEY = "hbase.mirror.table.state.to.zookeeper";

  public MirroringTableStateManager(MasterServices master) {
    super(master);
  }

  @Override
  protected void metaStateUpdated(TableName tableName, State newState) throws IOException {
    updateZooKeeper(new TableState(tableName, newState));
  }

  @Override
  protected void metaStateDeleted(TableName tableName) throws IOException {
    deleteZooKeeper(tableName);
  }

  private void updateZooKeeper(TableState tableState) throws IOException {
    if (tableState == null) {
      return;
    }
    String znode = ZNodePaths.joinZNode(this.master.getZooKeeper().getZNodePaths().tableZNode,
      tableState.getTableName().getNameAsString());
    try {
      // Make sure znode exists.
      if (ZKUtil.checkExists(this.master.getZooKeeper(), znode) == -1) {
        ZKUtil.createAndFailSilent(this.master.getZooKeeper(), znode);
      }
      // Now set newState
      ZooKeeperProtos.DeprecatedTableState.Builder builder =
        ZooKeeperProtos.DeprecatedTableState.newBuilder();
      builder.setState(
        ZooKeeperProtos.DeprecatedTableState.State.valueOf(tableState.getState().toString()));
      byte[] data = ProtobufUtil.prependPBMagic(builder.build().toByteArray());
      ZKUtil.setData(this.master.getZooKeeper(), znode, data);
    } catch (KeeperException e) {
      // Only hbase1 clients suffer if this fails.
      LOG.warn("Failed setting table state to zookeeper mirrored for hbase-1.x clients", e);
    }
  }

  // This method is called by the super class on each row it finds in the hbase:meta table with
  // table state in it.
  @Override
  protected void fixTableState(TableState tableState) throws IOException {
    updateZooKeeper(tableState);
  }
}

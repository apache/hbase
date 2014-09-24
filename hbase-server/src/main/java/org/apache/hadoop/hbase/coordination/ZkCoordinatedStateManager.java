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
package org.apache.hadoop.hbase.coordination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.zookeeper.ZKTableStateManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper-based implementation of {@link org.apache.hadoop.hbase.CoordinatedStateManager}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ZkCoordinatedStateManager extends BaseCoordinatedStateManager {
  private static final Log LOG = LogFactory.getLog(ZkCoordinatedStateManager.class);
  protected Server server;
  protected ZooKeeperWatcher watcher;
  protected SplitTransactionCoordination splitTransactionCoordination;
  protected CloseRegionCoordination closeRegionCoordination;
  protected SplitLogWorkerCoordination splitLogWorkerCoordination;
  protected SplitLogManagerCoordination splitLogManagerCoordination;
  protected OpenRegionCoordination openRegionCoordination;
  protected RegionMergeCoordination regionMergeCoordination;

  @Override
  public void initialize(Server server) {
    this.server = server;
    this.watcher = server.getZooKeeper();
    splitLogWorkerCoordination = new ZkSplitLogWorkerCoordination(this, watcher);
    splitLogManagerCoordination = new ZKSplitLogManagerCoordination(this, watcher);
    splitTransactionCoordination = new ZKSplitTransactionCoordination(this, watcher);
    closeRegionCoordination = new ZkCloseRegionCoordination(this, watcher);
    openRegionCoordination = new ZkOpenRegionCoordination(this, watcher);
    regionMergeCoordination = new ZkRegionMergeCoordination(this, watcher);
  }

  @Override
  public Server getServer() {
    return server;
  }

  @Override
  public TableStateManager getTableStateManager() throws InterruptedException,
      CoordinatedStateException {
    try {
      return new ZKTableStateManager(server.getZooKeeper());
    } catch (KeeperException e) {
      throw new CoordinatedStateException(e);
    }
  }

  @Override
  public SplitLogWorkerCoordination getSplitLogWorkerCoordination() {
    return splitLogWorkerCoordination;
    }
  @Override
  public SplitLogManagerCoordination getSplitLogManagerCoordination() {
    return splitLogManagerCoordination;
  }

  @Override
  public SplitTransactionCoordination getSplitTransactionCoordination() {
    return splitTransactionCoordination;
  }

  @Override
  public CloseRegionCoordination getCloseRegionCoordination() {
    return closeRegionCoordination;
  }

  @Override
  public OpenRegionCoordination getOpenRegionCoordination() {
    return openRegionCoordination;
  }

  @Override
  public RegionMergeCoordination getRegionMergeCoordination() {
    return regionMergeCoordination;
  }
}

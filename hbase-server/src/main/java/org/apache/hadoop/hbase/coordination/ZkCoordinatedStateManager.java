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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * ZooKeeper-based implementation of {@link org.apache.hadoop.hbase.CoordinatedStateManager}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ZkCoordinatedStateManager extends BaseCoordinatedStateManager {
  protected Server server;
  protected ZooKeeperWatcher watcher;
  protected SplitLogWorkerCoordination splitLogWorkerCoordination;
  protected SplitLogManagerCoordination splitLogManagerCoordination;

  @Override
  public void initialize(Server server) {
    this.server = server;
    this.watcher = server.getZooKeeper();
    splitLogWorkerCoordination = new ZkSplitLogWorkerCoordination(this, watcher);
    splitLogManagerCoordination = new ZKSplitLogManagerCoordination(this, watcher);

  }

  @Override
  public Server getServer() {
    return server;
  }

  @Override
  public SplitLogWorkerCoordination getSplitLogWorkerCoordination() {
    return splitLogWorkerCoordination;
    }
  @Override
  public SplitLogManagerCoordination getSplitLogManagerCoordination() {
    return splitLogManagerCoordination;
  }
}

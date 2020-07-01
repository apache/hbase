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

import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ZooKeeper-based implementation of {@link org.apache.hadoop.hbase.CoordinatedStateManager}.
 * @deprecated since 2.4.0 and in 3.0.0, to be removed in 4.0.0, replaced by procedure-based
 *   distributed WAL splitter (see SplitWALManager) which doesn't use this zk-based coordinator.
 */
@Deprecated
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ZkCoordinatedStateManager implements CoordinatedStateManager {
  protected ZKWatcher watcher;
  protected SplitLogWorkerCoordination splitLogWorkerCoordination;
  protected SplitLogManagerCoordination splitLogManagerCoordination;

  public ZkCoordinatedStateManager(Server server) {
    this.watcher = server.getZooKeeper();
    splitLogWorkerCoordination = new ZkSplitLogWorkerCoordination(server.getServerName(), watcher);
    splitLogManagerCoordination = new ZKSplitLogManagerCoordination(server.getConfiguration(),
        watcher);
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

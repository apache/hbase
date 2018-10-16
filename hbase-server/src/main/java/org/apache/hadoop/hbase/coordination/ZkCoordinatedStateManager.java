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

import java.io.IOException;

import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.hadoop.hbase.procedure.ZKProcedureCoordinator;
import org.apache.hadoop.hbase.procedure.ZKProcedureMemberRpcs;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper-based implementation of {@link org.apache.hadoop.hbase.CoordinatedStateManager}.
 */
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

  @Override
  public ProcedureCoordinatorRpcs getProcedureCoordinatorRpcs(String procType, String coordNode)
      throws IOException {
    return new ZKProcedureCoordinator(watcher, procType, coordNode);
  }

  @Override
  public ProcedureMemberRpcs getProcedureMemberRpcs(String procType) throws KeeperException {
    return new ZKProcedureMemberRpcs(watcher, procType);
  }
}

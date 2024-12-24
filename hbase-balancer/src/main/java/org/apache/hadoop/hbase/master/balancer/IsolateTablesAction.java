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
package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class IsolateTablesAction extends BalanceAction {
  private final int server;
  private final Set<TableName> tables;
  private final List<MoveRegionAction> moveActions;

  public IsolateTablesAction(BalancerClusterState cluster, int server,
    Set<TableName> tablesToIsolate) {
    super(Type.ISOLATE_TABLE);
    this.server = server;
    this.tables = new HashSet<>(tablesToIsolate);
    this.moveActions = new ArrayList<>();

    // Identify regions to move and generate MoveRegionActions
    for (int regionIndex : cluster.regionsPerServer[server]) {
      RegionInfo regionInfo = cluster.regions[regionIndex];
      if (tables.contains(regionInfo.getTable())) {
        continue; // Skip regions belonging to the tables to isolate
      }

      // Select a deterministic target server
      Set<Integer> badTargets = BalancerConditionals.INSTANCE.getServersWithTablesToIsolate();
      boolean hasGoodTargets = cluster.numServers > badTargets.size();
      if (hasGoodTargets) {
        // Pick good target
        while (true) {
          int randomOtherServer = pickRandomServer(cluster, server);
          if (!badTargets.contains(randomOtherServer)) {
            moveActions.add(new MoveRegionAction(regionIndex, server, randomOtherServer));
            break;
          }
        }
      } else {
        // Pick any valid target
        moveActions
          .add(new MoveRegionAction(regionIndex, server, pickRandomServer(cluster, server)));
      }
    }
  }

  @Override
  long getStepCount() {
    return moveActions.size();
  }

  public IsolateTablesAction(int server, Set<TableName> tablesToIsolate,
    List<MoveRegionAction> moveActions) {
    super(Type.ISOLATE_TABLE);
    this.server = server;
    this.tables = new HashSet<>(tablesToIsolate);
    this.moveActions = moveActions;
  }

  private int pickRandomServer(BalancerClusterState cluster, int excludedServer) {
    int randomServer = excludedServer;
    while (randomServer == excludedServer) {
      randomServer = ThreadLocalRandom.current().nextInt(cluster.numServers);
    }
    return randomServer;
  }

  public int getServer() {
    return server;
  }

  public List<MoveRegionAction> getMoveActions() {
    return moveActions;
  }

  @Override
  public BalanceAction undoAction() {
    List<MoveRegionAction> undoActions = new ArrayList<>();
    for (MoveRegionAction moveAction : moveActions) {
      undoActions.add((MoveRegionAction) moveAction.undoAction());
    }
    return new IsolateTablesAction(server, tables, undoActions);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType()).append(": Isolating tables ").append(tables).append(" on server ")
      .append(server).append(" with actions:\n");
    for (MoveRegionAction action : moveActions) {
      sb.append("  ").append(action).append("\n");
    }
    return sb.toString();
  }
}

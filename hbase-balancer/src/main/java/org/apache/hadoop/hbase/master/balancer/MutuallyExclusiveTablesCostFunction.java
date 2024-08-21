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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.*;

/**
 * Ensure Tables are mutually exclusive on an RS
 * for example: META and SYSTEM.CATALOG can be mutually exclusive Tables
 * on same RS
 */
@InterfaceAudience.Private public class MutuallyExclusiveTablesCostFunction extends CostFunction {
  public static final String MUTUALLY_EXCLUSIVE_TABLES_KEY =
    "hbase.master.balancer.stochastic.mutuallyExclusiveTables";
  public static final String MUTUALLY_EXCLUSIVE_TABLES_COST_KEY =
    "hbase.master.balancer.stochastic.mutuallyExclusiveTablesCost";
  public static final float DEFAULT_MUTUALLY_EXCLUSIVE_TABLES_COST = 10000;
  private static final int VIOLATION_COST = 1000;
  private Set<String> mutuallyExclusiveTables = Collections.emptySet();

  public MutuallyExclusiveTablesCostFunction(Configuration conf) {
    this.setMultiplier(
      conf.getFloat(MUTUALLY_EXCLUSIVE_TABLES_COST_KEY, DEFAULT_MUTUALLY_EXCLUSIVE_TABLES_COST));
    initializeMutuallyExclusiveTables(conf);
  }

  private void initializeMutuallyExclusiveTables(Configuration conf) {
    String[] tables = conf.getStrings(MUTUALLY_EXCLUSIVE_TABLES_KEY);
    if (tables != null && tables.length > 0) {
      this.mutuallyExclusiveTables = new HashSet<>(Arrays.asList(tables));
    }
  }

  @Override void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
  }

  @Override protected double cost() {
    double totalCost = 0;

    Map<ServerName, List<RegionInfo>> clusterState = this.cluster.clusterState;

    for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
      List<RegionInfo> regions = entry.getValue();
      Set<String> exclusiveTablesOnServer = new HashSet<>();
      for (RegionInfo regionInfo : regions) {
        String tableName = regionInfo.getTable().getNameAsString();
        if (mutuallyExclusiveTables.contains(tableName)) {
          // If its already seen table, continue
          if (exclusiveTablesOnServer.contains(tableName)) {
            continue;
          }
          exclusiveTablesOnServer.add(tableName);
          if (exclusiveTablesOnServer.size() > 1) {
            totalCost += VIOLATION_COST;
          }
        }
      }
    }

    return totalCost;
  }
}

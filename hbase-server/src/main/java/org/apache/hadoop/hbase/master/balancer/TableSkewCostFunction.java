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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Compute the cost of a potential cluster configuration based upon how evenly distributed tables
 * are.
 */
@InterfaceAudience.Private
class TableSkewCostFunction extends CostFunction {

  private static final String TABLE_SKEW_COST_KEY =
    "hbase.master.balancer.stochastic.tableSkewCost";
  private static final float DEFAULT_TABLE_SKEW_COST = 35;

  TableSkewCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(TABLE_SKEW_COST_KEY, DEFAULT_TABLE_SKEW_COST));
  }

  @Override
  protected double cost() {
    if (cluster.numTables <= 1) {
      return 0;
    }
    double max = cluster.numRegions;
    double min = ((double) cluster.numRegions) / cluster.numServers;
    double value = 0;

    for (int i = 0; i < cluster.numMaxRegionsPerTable.length; i++) {
      value += cluster.numMaxRegionsPerTable[i];
    }

    return scale(min, max, value);
  }
}

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

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class TableIsolationCandidateGenerator
  extends RegionPlanConditionalCandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(TableIsolationCandidateGenerator.class);

  abstract boolean shouldBeIsolated(RegionInfo regionInfo);

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    return generateCandidate(cluster, false);
  }

  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    if (
      !BalancerConditionals.INSTANCE.isSystemTableIsolationEnabled()
        && !BalancerConditionals.INSTANCE.isMetaTableIsolationEnabled()
    ) {
      return BalanceAction.NULL_ACTION;
    }

    for (int serverIdx = 0; serverIdx < cluster.numServers; serverIdx++) {
      Set<TableName> tablesToIsolate = new HashSet<>();
      boolean hasRegionsToIsolate = false;
      boolean hasRegionsToMove = false;
      for (int regionIdx : cluster.regionsPerServer[serverIdx]) {
        RegionInfo regionInfo = cluster.regions[regionIdx];
        if (shouldBeIsolated(regionInfo)) {
          hasRegionsToIsolate = true;
          tablesToIsolate.add(regionInfo.getTable());
        } else {
          hasRegionsToMove = true;
        }
      }
      if (hasRegionsToMove && hasRegionsToIsolate) {
        return new IsolateTablesAction(cluster, serverIdx, tablesToIsolate);
      }
    } // todo rmattingly can we use move batch to make this smarter?

    return BalanceAction.NULL_ACTION;
  }
}

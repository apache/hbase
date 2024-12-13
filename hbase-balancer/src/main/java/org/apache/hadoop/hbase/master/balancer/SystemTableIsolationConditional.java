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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * If enabled, this class will help the balancer ensure that system tables live on their own
 * RegionServer. System tables will share one RegionServer! This conditional can be used in tandem
 * with {@link MetaTableIsolationConditional} to add a second RegionServer specifically for meta
 * table hosting. Configure this via {@link BalancerConditionals#ISOLATE_SYSTEM_TABLES_KEY}
 */
@InterfaceAudience.Private
class SystemTableIsolationConditional extends RegionPlanConditional {

  private final Set<TableName> systemTables;

  public SystemTableIsolationConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);
    boolean isolateMeta = conf.getBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, false);
    SystemTableIsolationCandidateGenerator.INSTANCE.setIsolateMeta(isolateMeta);
    systemTables = cluster.tables.stream().map(TableName::valueOf).filter(TableName::isSystemTable)
      .filter(t -> !isolateMeta || !t.equals(TableName.META_TABLE_NAME))
      .collect(Collectors.toSet());
  }

  @Override
  List<RegionPlanConditionalCandidateGenerator> getCandidateGenerators() {
    List<RegionPlanConditionalCandidateGenerator> generators =
      new ArrayList<>(systemTables.size() + 1);
    generators.add(SystemTableIsolationCandidateGenerator.INSTANCE);
    for (TableName systemTable : systemTables) {
      generators.add(new TableColocationCandidateGenerator(systemTable));
    }
    return generators;
  }

  @Override
  public boolean isViolatingServer(RegionPlan regionPlan, Set<RegionInfo> serverRegions) {
    RegionInfo regionBeingMoved = regionPlan.getRegionInfo();
    boolean shouldIsolateMovingRegion = isRegionToIsolate(regionBeingMoved);
    for (RegionInfo destinationRegion : serverRegions) {
      if (destinationRegion.getEncodedName().equals(regionBeingMoved.getEncodedName())) {
        // Skip the region being moved
        continue;
      }
      if (shouldIsolateMovingRegion && !isRegionToIsolate(destinationRegion)) {
        // Ensure every destination region is also a region to isolate
        return true;
      } else if (!shouldIsolateMovingRegion && isRegionToIsolate(destinationRegion)) {
        // Ensure no destination region is a region to isolate
        return true;
      }
    }
    return false;
  }

  private boolean isRegionToIsolate(RegionInfo regionInfo) {
    boolean isRegionToIsolate = false;
    if (BalancerConditionals.INSTANCE.isMetaTableIsolationEnabled() && regionInfo.isMetaRegion()) {
      isRegionToIsolate = true;
    } else if (regionInfo.getTable().isSystemTable()) {
      isRegionToIsolate = true;
    }
    return isRegionToIsolate;
  }

}

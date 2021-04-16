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
package org.apache.hadoop.hbase.master.balancer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * a balancer which is only used in maintenance mode.
 */
@InterfaceAudience.Private
public class MaintenanceLoadBalancer extends Configured implements LoadBalancer {

  private volatile boolean stopped = false;

  @Override
  public void stop(String why) {
    stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public void setClusterMetrics(ClusterMetrics st) {
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
  }

  @Override
  public List<RegionPlan> balanceCluster(
    Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) throws IOException {
    // do not need to balance in maintenance mode
    return Collections.emptyList();
  }

  @Override
  public List<RegionPlan> balanceTable(TableName tableName,
    Map<ServerName, List<RegionInfo>> loadOfOneTable) {
    return Collections.emptyList();
  }

  private Map<ServerName, List<RegionInfo>> assign(Collection<RegionInfo> regions,
    List<ServerName> servers) {
    // should only have 1 region server in maintenance mode
    assert servers.size() == 1;
    List<RegionInfo> systemRegions =
      regions.stream().filter(r -> r.getTable().isSystemTable()).collect(Collectors.toList());
    if (!systemRegions.isEmpty()) {
      return Collections.singletonMap(servers.get(0), systemRegions);
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
    List<ServerName> servers) {
    return assign(regions, servers);
  }

  @Override
  public Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
    List<ServerName> servers)  {
    return assign(regions.keySet(), servers);
  }

  @Override
  public ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers) {
    // should only have 1 region server in maintenance mode
    assert servers.size() == 1;
    return regionInfo.getTable().isSystemTable() ? servers.get(0) : null;
  }

  @Override
  public void initialize() {
  }

  @Override
  public void regionOnline(RegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(RegionInfo regionInfo) {
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
  }

  @Override
  public void postMasterStartupInitialize() {
  }

  @Override
  public void updateBalancerStatus(boolean status) {
  }
}

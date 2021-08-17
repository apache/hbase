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
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalancerDecision;
import org.apache.hadoop.hbase.client.BalancerRejection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;

public class DummyClusterInfoProvider implements ClusterInfoProvider {

  private volatile Configuration conf;

  public DummyClusterInfoProvider(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Connection getConnection() {
    return null;
  }

  @Override
  public List<RegionInfo> getAssignedRegions() {
    return Collections.emptyList();
  }

  @Override
  public void unassign(RegionInfo regionInfo) throws IOException {
  }

  @Override
  public TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public int getNumberOfTables() throws IOException {
    return 0;
  }

  @Override
  public HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
    TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException {
    return new HDFSBlocksDistribution();
  }

  @Override
  public boolean hasRegionReplica(Collection<RegionInfo> regions) throws IOException {
    return false;
  }

  @Override
  public List<ServerName> getOnlineServersList() {
    return Collections.emptyList();
  }

  @Override
  public List<ServerName> getOnlineServersListWithPredicator(List<ServerName> servers,
    Predicate<ServerMetrics> filter) {
    return Collections.emptyList();
  }

  @Override
  public Map<ServerName, List<RegionInfo>> getSnapShotOfAssignment(Collection<RegionInfo> regions) {
    return Collections.emptyMap();
  }

  @Override
  public boolean isOffPeakHour() {
    return false;
  }

  @Override
  public void recordBalancerDecision(Supplier<BalancerDecision> decision) {
  }

  @Override
  public void recordBalancerRejection(Supplier<BalancerRejection> rejection) {
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public ServerMetrics getLoad(ServerName serverName) {
    return null;
  }
}

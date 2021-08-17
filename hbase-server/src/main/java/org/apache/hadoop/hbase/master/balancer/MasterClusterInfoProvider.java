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

import com.google.errorprone.annotations.RestrictedApi;
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
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalancerDecision;
import org.apache.hadoop.hbase.client.BalancerRejection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.namequeues.BalancerDecisionDetails;
import org.apache.hadoop.hbase.namequeues.BalancerRejectionDetails;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Master based cluster info provider.
 */
@InterfaceAudience.Private
public class MasterClusterInfoProvider implements ClusterInfoProvider {

  private final MasterServices services;

  private boolean isBalancerDecisionRecording;

  private boolean isBalancerRejectionRecording;

  /**
   * Use to add balancer decision history to ring-buffer
   */
  private NamedQueueRecorder namedQueueRecorder;

  private OffPeakHours offPeakHours;

  private void loadConf(Configuration conf) {
    this.offPeakHours = OffPeakHours.getInstance(conf);
    isBalancerDecisionRecording = conf.getBoolean(BaseLoadBalancer.BALANCER_DECISION_BUFFER_ENABLED,
      BaseLoadBalancer.DEFAULT_BALANCER_DECISION_BUFFER_ENABLED);
    isBalancerRejectionRecording =
      conf.getBoolean(BaseLoadBalancer.BALANCER_REJECTION_BUFFER_ENABLED,
        BaseLoadBalancer.DEFAULT_BALANCER_REJECTION_BUFFER_ENABLED);
    if (isBalancerDecisionRecording || isBalancerRejectionRecording) {
      this.namedQueueRecorder = NamedQueueRecorder.getInstance(conf);
    } else {
      this.namedQueueRecorder = null;
    }
  }

  public MasterClusterInfoProvider(MasterServices services) {
    this.services = services;
    loadConf(services.getConfiguration());
  }

  @Override
  public Configuration getConfiguration() {
    return services.getConfiguration();
  }

  @Override
  public Connection getConnection() {
    return services.getConnection();
  }

  @Override
  public List<RegionInfo> getAssignedRegions() {
    AssignmentManager am = services.getAssignmentManager();
    return am != null ? am.getAssignedRegions() : Collections.emptyList();
  }

  @Override
  public void unassign(RegionInfo regionInfo) throws IOException {
    AssignmentManager am = services.getAssignmentManager();
    if (am != null) {
      am.unassign(regionInfo);
    }
  }

  @Override
  public TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    TableDescriptors tds = services.getTableDescriptors();
    return tds != null ? tds.get(tableName) : null;
  }

  @Override
  public HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
    TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException {
    return HRegion.computeHDFSBlocksDistribution(conf, tableDescriptor, regionInfo);
  }

  @Override
  public boolean hasRegionReplica(Collection<RegionInfo> regions) throws IOException {
    TableDescriptors tds = services.getTableDescriptors();
    if (tds == null) {
      return false;
    }
    for (RegionInfo region : regions) {
      TableDescriptor td = tds.get(region.getTable());
      if (td != null && td.getRegionReplication() > 1) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<ServerName> getOnlineServersList() {
    ServerManager sm = services.getServerManager();
    return sm != null ? sm.getOnlineServersList() : Collections.emptyList();
  }

  @Override
  public List<ServerName> getOnlineServersListWithPredicator(List<ServerName> servers,
    Predicate<ServerMetrics> filter) {
    ServerManager sm = services.getServerManager();
    return sm != null ? sm.getOnlineServersListWithPredicator(servers, filter) :
      Collections.emptyList();
  }

  @Override
  public Map<ServerName, List<RegionInfo>> getSnapShotOfAssignment(Collection<RegionInfo> regions) {
    AssignmentManager am = services.getAssignmentManager();
    return am != null ? am.getSnapShotOfAssignment(regions) : Collections.emptyMap();
  }

  @Override
  public int getNumberOfTables() throws IOException {
    return services.getTableDescriptors().getAll().size();
  }

  @Override
  public boolean isOffPeakHour() {
    return offPeakHours.isOffPeakHour();
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    loadConf(conf);
  }

  @Override
  public void recordBalancerDecision(Supplier<BalancerDecision> decision) {
    if (isBalancerDecisionRecording) {
      namedQueueRecorder.addRecord(new BalancerDecisionDetails(decision.get()));
    }
  }

  @Override
  public void recordBalancerRejection(Supplier<BalancerRejection> rejection) {
    if (isBalancerRejectionRecording) {
      namedQueueRecorder.addRecord(new BalancerRejectionDetails(rejection.get()));
    }
  }

  @Override
  public ServerMetrics getLoad(ServerName serverName) {
    ServerManager sm = services.getServerManager();
    return sm != null ? sm.getLoad(serverName) : null;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  NamedQueueRecorder getNamedQueueRecorder() {
    return namedQueueRecorder;
  }
}

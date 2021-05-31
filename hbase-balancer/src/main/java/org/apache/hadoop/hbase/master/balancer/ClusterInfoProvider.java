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
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the cluster we want to balance. It provides methods to let us get the information we
 * want.
 */
@InterfaceAudience.Private
public interface ClusterInfoProvider extends ConfigurationObserver {

  /**
   * Get the configuration.
   */
  Configuration getConfiguration();

  /**
   * Returns a reference to the cluster's connection.
   */
  Connection getConnection();

  /**
   * Get all the regions of this cluster.
   * <p/>
   * Used to refresh region block locations on HDFS.
   */
  List<RegionInfo> getAssignedRegions();

  /**
   * Unassign the given region.
   */
  void unassign(RegionInfo regionInfo) throws IOException;

  /**
   * Get the table descriptor for the given table.
   */
  TableDescriptor getTableDescriptor(TableName tableName) throws IOException;

  /**
   * Returns the number of tables on this cluster.
   */
  int getNumberOfTables() throws IOException;

  /**
   * Compute the block distribution for the given region.
   * <p/>
   * Used to refresh region block locations on HDFS.
   */
  HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
    TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException;

  /**
   * Check whether we have region replicas enabled for the tables of the given regions.
   */
  boolean hasRegionReplica(Collection<RegionInfo> regions) throws IOException;

  /**
   * Returns a copy of the internal list of online servers.
   */
  List<ServerName> getOnlineServersList();

  /**
   * Returns a copy of the internal list of online servers matched by the given {@code filter}.
   */
  List<ServerName> getOnlineServersListWithPredicator(List<ServerName> servers,
    Predicate<ServerMetrics> filter);

  /**
   * Get a snapshot of the current assignment status.
   */
  Map<ServerName, List<RegionInfo>> getSnapShotOfAssignment(Collection<RegionInfo> regions);

  /**
   * Test whether we are in off peak hour.
   * <p/>
   * For peak and off peak hours we may have different cost for the same balancing operation.
   */
  boolean isOffPeakHour();

  /**
   * Record the given balancer decision.
   */
  void recordBalancerDecision(Supplier<BalancerDecision> decision);

  /**
   * Record the given balancer rejection.
   */
  void recordBalancerRejection(Supplier<BalancerRejection> rejection);

  /**
   * Returns server metrics of the given server if serverName is known else null
   */
  ServerMetrics getLoad(ServerName serverName);
}

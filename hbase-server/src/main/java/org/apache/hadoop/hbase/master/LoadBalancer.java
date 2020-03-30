/**
 *
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
package org.apache.hadoop.hbase.master;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Makes decisions about the placement and movement of Regions across
 * RegionServers.
 *
 * <p>Cluster-wide load balancing will occur only when there are no regions in
 * transition and according to a fixed period of a time using {@link #balanceCluster(Map)}.
 *
 * <p>On cluster startup, bulk assignment can be used to determine
 * locations for all Regions in a cluster.
 *
 * <p>This class produces plans for the
 * {@link org.apache.hadoop.hbase.master.assignment.AssignmentManager}
 * to execute.
 */
@InterfaceAudience.Private
public interface LoadBalancer extends Configurable, Stoppable, ConfigurationObserver {
  /**
   * Master can carry regions as of hbase-2.0.0.
   * By default, it carries no tables.
   * TODO: Add any | system as flags to indicate what it can do.
   */
  String TABLES_ON_MASTER = "hbase.balancer.tablesOnMaster";

  /**
   * Master carries system tables.
   */
  String SYSTEM_TABLES_ON_MASTER = "hbase.balancer.tablesOnMaster.systemTablesOnly";

  // Used to signal to the caller that the region(s) cannot be assigned
  // We deliberately use 'localhost' so the operation will fail fast
  ServerName BOGUS_SERVER_NAME = ServerName.valueOf("localhost,1,1");

  /**
   * Config for pluggable load balancers.
   * @deprecated since 3.0.0, will be removed in 4.0.0. In the new implementation, as the base load
   *             balancer will always be the rs group based one, you should just use
   *             {@link org.apache.hadoop.hbase.HConstants#HBASE_MASTER_LOADBALANCER_CLASS} to
   *             config the per group load balancer.
   */
  @Deprecated
  String HBASE_RSGROUP_LOADBALANCER_CLASS = "hbase.rsgroup.grouploadbalancer.class";
  /**
   * Set the current cluster status. This allows a LoadBalancer to map host name to a server
   */
  void setClusterMetrics(ClusterMetrics st);


  /**
   * Set the master service.
   */
  void setMasterServices(MasterServices masterServices);

  /**
   * Perform the major balance operation for cluster, will invoke {@link #balanceTable} to do actual
   * balance. Normally not need override this method, except
   * {@link org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer} and
   * {@link org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer}
   * @param loadOfAllTable region load of servers for all table
   * @return a list of regions to be moved, including source and destination, or null if cluster is
   *         already balanced
   */
  List<RegionPlan> balanceCluster(Map<TableName,
      Map<ServerName, List<RegionInfo>>> loadOfAllTable) throws IOException;

  /**
   * Perform the major balance operation for table, all class implement of {@link LoadBalancer}
   * should override this method
   * @param tableName the table to be balanced
   * @param loadOfOneTable region load of servers for the specific one table
   * @return List of plans
   */
  List<RegionPlan> balanceTable(TableName tableName,
      Map<ServerName, List<RegionInfo>> loadOfOneTable);

  /**
   * Perform a Round Robin assignment of regions.
   * @return Map of servername to regioninfos
   */
  Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
      List<ServerName> servers) throws IOException;

  /**
   * Assign regions to the previously hosting region server
   * @return List of plans
   */
  @Nullable
  Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
      List<ServerName> servers) throws IOException;

  /**
   * Get a random region server from the list
   * @param regionInfo Region for which this selection is being done.
   */
  ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers) throws IOException;

  /**
   * Initialize the load balancer. Must be called after setters.
   */
  void initialize() throws IOException;

  /**
   * Marks the region as online at balancer.
   */
  void regionOnline(RegionInfo regionInfo, ServerName sn);

  /**
   * Marks the region as offline at balancer.
   */
  void regionOffline(RegionInfo regionInfo);

  /**
   * Notification that config has changed
   */
  @Override
  void onConfigurationChange(Configuration conf);

  /**
   * If balancer needs to do initialization after Master has started up, lets do that here.
   */
  void postMasterStartupInitialize();

  /*Updates balancer status tag reported to JMX*/
  void updateBalancerStatus(boolean status);

  /**
   * @return true if Master carries regions
   */
  static boolean isTablesOnMaster(Configuration conf) {
    return conf.getBoolean(TABLES_ON_MASTER, false);
  }

  static boolean isSystemTablesOnlyOnMaster(Configuration conf) {
    return conf.getBoolean(SYSTEM_TABLES_ON_MASTER, false);
  }

  static boolean isMasterCanHostUserRegions(Configuration conf) {
    return isTablesOnMaster(conf) && !isSystemTablesOnlyOnMaster(conf);
  }
}

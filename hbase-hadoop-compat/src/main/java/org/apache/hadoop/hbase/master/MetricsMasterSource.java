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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.metrics.OperationMetrics;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that classes that expose metrics about the master will implement.
 */
@InterfaceAudience.Private
public interface MetricsMasterSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Server";

  /**
   * The context metrics will be under.
   */
  String METRICS_CONTEXT = "master";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase master server";

  // Strings used for exporting to metrics system.
  String MASTER_ACTIVE_TIME_NAME = "masterActiveTime";
  String MASTER_START_TIME_NAME = "masterStartTime";
  String MASTER_FINISHED_INITIALIZATION_TIME_NAME = "masterFinishedInitializationTime";
  String AVERAGE_LOAD_NAME = "averageLoad";
  String LIVE_REGION_SERVERS_NAME = "liveRegionServers";
  String DEAD_REGION_SERVERS_NAME = "deadRegionServers";
  String DRAINING_REGION_SERVER_NAME = "draininigRegionServers";
  String NUM_REGION_SERVERS_NAME = "numRegionServers";
  String NUM_DEAD_REGION_SERVERS_NAME = "numDeadRegionServers";
  String NUM_DRAINING_REGION_SERVERS_NAME = "numDrainingRegionServers";
  String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  String SERVER_NAME_NAME = "serverName";
  String CLUSTER_ID_NAME = "clusterId";
  String IS_ACTIVE_MASTER_NAME = "isActiveMaster";
  String SPLIT_PLAN_COUNT_NAME = "splitPlanCount";
  String MERGE_PLAN_COUNT_NAME = "mergePlanCount";

  String CLUSTER_REQUESTS_NAME = "clusterRequests";
  String MASTER_ACTIVE_TIME_DESC = "Master Active Time";
  String MASTER_START_TIME_DESC = "Master Start Time";
  String MASTER_FINISHED_INITIALIZATION_TIME_DESC = "Timestamp when Master has finished initializing";
  String AVERAGE_LOAD_DESC = "AverageLoad";
  String LIVE_REGION_SERVERS_DESC = "Names of live RegionServers";
  String NUMBER_OF_REGION_SERVERS_DESC = "Number of RegionServers";
  String DEAD_REGION_SERVERS_DESC = "Names of dead RegionServers";
  String NUMBER_OF_DEAD_REGION_SERVERS_DESC = "Number of dead RegionServers";
  String DRAINING_REGION_SERVER_DESC = "Names of draining RegionServers";
  String NUMBER_OF_DRAINING_REGION_SERVERS_DESC = "Number of draining RegionServers";
  String ZOOKEEPER_QUORUM_DESC = "ZooKeeper Quorum";
  String SERVER_NAME_DESC = "Server Name";
  String CLUSTER_ID_DESC = "Cluster Id";
  String IS_ACTIVE_MASTER_DESC = "Is Active Master";
  String SPLIT_PLAN_COUNT_DESC = "Number of Region Split Plans executed";
  String MERGE_PLAN_COUNT_DESC = "Number of Region Merge Plans executed";

  String SERVER_CRASH_METRIC_PREFIX = "serverCrash";

  /**
   * Increment the number of requests the cluster has seen.
   *
   * @param inc Ammount to increment the total by.
   */
  void incRequests(final long inc);

  /**
   * @return {@link OperationMetrics} containing common metrics for server crash operation
   */
  OperationMetrics getServerCrashMetrics();
}

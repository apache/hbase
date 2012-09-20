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

package org.apache.hadoop.hbase.master.metrics;

import org.apache.hadoop.hbase.metrics.BaseMetricsSource;

/**
 * Interface that classes that expose metrics about the master will implement.
 */
public interface MasterMetricsSource extends BaseMetricsSource {

  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "HMaster";

  /**
   * The context metrics will be under.
   */
  static final String METRICS_CONTEXT = "hmaster";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  static final String METRICS_JMX_CONTEXT = "HMaster";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about HBase master server";

  // Strings used for exporting to metrics system.
  static final String MASTER_ACTIVE_TIME_NAME = "masterActiveTime";
  static final String MASTER_START_TIME_NAME = "masterStartTime";
  static final String AVERAGE_LOAD_NAME = "averageLoad";
  static final String NUM_REGION_SERVERS_NAME = "numRegionServers";
  static final String NUM_DEAD_REGION_SERVERS_NAME = "numDeadRegionServers";
  static final String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  static final String SERVER_NAME_NAME = "serverName";
  static final String CLUSTER_ID_NAME = "clusterId";
  static final String IS_ACTIVE_MASTER_NAME = "isActiveMaster";
  static final String SPLIT_TIME_NAME = "hlogSplitTime";
  static final String SPLIT_SIZE_NAME = "hlogSplitSize";
  static final String CLUSTER_REQUESTS_NAME = "clusterRequests";
  static final String RIT_COUNT_NAME = "ritCount";
  static final String RIT_COUNT_OVER_THRESHOLD_NAME = "ritCountOverThreshold";
  static final String RIT_OLDEST_AGE_NAME = "ritOldestAge";
  static final String MASTER_ACTIVE_TIME_DESC = "Master Active Time";
  static final String MASTER_START_TIME_DESC = "Master Start Time";
  static final String AVERAGE_LOAD_DESC = "AverageLoad";
  static final String NUMBER_OF_REGION_SERVERS_DESC = "Number of RegionServers";
  static final String NUMBER_OF_DEAD_REGION_SERVERS_DESC = "Number of dead RegionServers";
  static final String ZOOKEEPER_QUORUM_DESC = "Zookeeper Quorum";
  static final String SERVER_NAME_DESC = "Server Name";
  static final String CLUSTER_ID_DESC = "Cluster Id";
  static final String IS_ACTIVE_MASTER_DESC = "Is Active Master";
  static final String SPLIT_TIME_DESC = "Time it takes to finish HLog.splitLog()";
  static final String SPLIT_SIZE_DESC = "Size of HLog files being split";


  /**
   * Increment the number of requests the cluster has seen.
   * @param inc Ammount to increment the total by.
   */
  void incRequests(final int inc);

  /**
   * Set the number of regions in transition.
   * @param ritCount count of the regions in transition.
   */
  void setRIT(int ritCount);

  /**
   * Set the count of the number of regions that have been in transition over the threshold time.
   * @param ritCountOverThreshold number of regions in transition for longer than threshold.
   */
  void setRITCountOverThreshold(int ritCountOverThreshold);

  /**
   * Set the oldest region in transition.
   * @param age age of the oldest RIT.
   */
  void setRITOldestAge(long age);

  void updateSplitTime(long time);

  void updateSplitSize(long size);

}

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

/**
 * Interface that classes that expose metrics about the master will implement.
 */
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
  String AVERAGE_LOAD_NAME = "averageLoad";
  String NUM_REGION_SERVERS_NAME = "numRegionServers";
  String NUM_DEAD_REGION_SERVERS_NAME = "numDeadRegionServers";
  String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  String SERVER_NAME_NAME = "serverName";
  String CLUSTER_ID_NAME = "clusterId";
  String IS_ACTIVE_MASTER_NAME = "isActiveMaster";
  String SPLIT_TIME_NAME = "hlogSplitTime";
  String SPLIT_SIZE_NAME = "hlogSplitSize";
  String SNAPSHOT_TIME_NAME = "snapshotTime";
  String SNAPSHOT_RESTORE_TIME_NAME = "snapshotRestoreTime";
  String SNAPSHOT_CLONE_TIME_NAME = "snapshotCloneTime";
  String META_SPLIT_TIME_NAME = "metaHlogSplitTime";
  String META_SPLIT_SIZE_NAME = "metaHlogSplitSize";
  String CLUSTER_REQUESTS_NAME = "clusterRequests";
  String RIT_COUNT_NAME = "ritCount";
  String RIT_COUNT_OVER_THRESHOLD_NAME = "ritCountOverThreshold";
  String RIT_OLDEST_AGE_NAME = "ritOldestAge";
  String MASTER_ACTIVE_TIME_DESC = "Master Active Time";
  String MASTER_START_TIME_DESC = "Master Start Time";
  String AVERAGE_LOAD_DESC = "AverageLoad";
  String NUMBER_OF_REGION_SERVERS_DESC = "Number of RegionServers";
  String NUMBER_OF_DEAD_REGION_SERVERS_DESC = "Number of dead RegionServers";
  String ZOOKEEPER_QUORUM_DESC = "Zookeeper Quorum";
  String SERVER_NAME_DESC = "Server Name";
  String CLUSTER_ID_DESC = "Cluster Id";
  String IS_ACTIVE_MASTER_DESC = "Is Active Master";
  String SPLIT_TIME_DESC = "Time it takes to finish HLog.splitLog()";
  String SPLIT_SIZE_DESC = "Size of HLog files being split";
  String SNAPSHOT_TIME_DESC = "Time it takes to finish snapshot()";
  String SNAPSHOT_RESTORE_TIME_DESC = "Time it takes to finish restoreSnapshot()";
  String SNAPSHOT_CLONE_TIME_DESC = "Time it takes to finish cloneSnapshot()";
  String META_SPLIT_TIME_DESC = "Time it takes to finish splitMetaLog()";
  String META_SPLIT_SIZE_DESC = "Size of META HLog files being split";

  /**
   * Increment the number of requests the cluster has seen.
   *
   * @param inc Ammount to increment the total by.
   */
  void incRequests(final int inc);

  /**
   * Set the number of regions in transition.
   *
   * @param ritCount count of the regions in transition.
   */
  void setRIT(int ritCount);

  /**
   * Set the count of the number of regions that have been in transition over the threshold time.
   *
   * @param ritCountOverThreshold number of regions in transition for longer than threshold.
   */
  void setRITCountOverThreshold(int ritCountOverThreshold);

  /**
   * Set the oldest region in transition.
   *
   * @param age age of the oldest RIT.
   */
  void setRITOldestAge(long age);

  void updateSplitTime(long time);

  void updateSplitSize(long size);

  void updateSnapshotTime(long time);

  void updateSnapshotCloneTime(long time);

  void updateSnapshotRestoreTime(long time);
  
  void updateMetaWALSplitTime(long time);

  void updateMetaWALSplitSize(long size);

}

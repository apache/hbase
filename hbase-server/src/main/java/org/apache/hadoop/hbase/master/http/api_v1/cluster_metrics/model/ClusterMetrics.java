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
package org.apache.hadoop.hbase.master.http.api_v1.cluster_metrics.model;

import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Exposes a subset of fields from {@link org.apache.hadoop.hbase.ClusterMetrics}.
 */
@InterfaceAudience.Private
public final class ClusterMetrics {

  private final String hbaseVersion;
  private final String clusterId;
  private final ServerName masterName;
  private final List<ServerName> backupMasterNames;

  public static ClusterMetrics from(org.apache.hadoop.hbase.ClusterMetrics clusterMetrics) {
    return new ClusterMetrics(
      clusterMetrics.getHBaseVersion(),
      clusterMetrics.getClusterId(),
      clusterMetrics.getMasterName(),
      clusterMetrics.getBackupMasterNames());
  }

  private ClusterMetrics(
    String hbaseVersion,
    String clusterId,
    ServerName masterName,
    List<ServerName> backupMasterNames
  ) {
    this.hbaseVersion = hbaseVersion;
    this.clusterId = clusterId;
    this.masterName = masterName;
    this.backupMasterNames = backupMasterNames;
  }

  public String getHBaseVersion() {
    return hbaseVersion;
  }

  public String getClusterId() {
    return clusterId;
  }

  public ServerName getMasterName() {
    return masterName;
  }

  public List<ServerName> getBackupMasterNames() {
    return backupMasterNames;
  }
}

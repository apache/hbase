/*
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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Source that does nothing at all, helpful to test ReplicationSourceManager
 */
public class ReplicationSourceDummy implements ReplicationSourceInterface {

  ReplicationSourceManager manager;
  String peerClusterId;
  Path currentPath;
  MetricsSource metrics;
  public static final String fakeExceptionMessage = "Fake Exception";

  @Override
  public void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager,
      ReplicationQueues rq, ReplicationPeers rp, Stoppable stopper, String peerClusterId,
      UUID clusterId, ReplicationEndpoint replicationEndpoint, MetricsSource metrics)
          throws IOException {

    this.manager = manager;
    this.peerClusterId = peerClusterId;
    this.metrics = metrics;
  }

  @Override
  public void enqueueLog(Path log) {
    this.currentPath = log;
    metrics.incrSizeOfLogQueue();
  }

  @Override
  public Path getCurrentPath() {
    return this.currentPath;
  }

  @Override
  public void startup() {

  }

  @Override
  public void terminate(String reason) {

  }

  @Override
  public void terminate(String reason, Exception e) {

  }

  @Override
  public String getPeerClusterZnode() {
    return peerClusterId;
  }

  @Override
  public String getPeerClusterId() {
    String[] parts = peerClusterId.split("-", 2);
    return parts.length != 1 ?
        parts[0] : peerClusterId;
  }

  @Override
  public String getStats() {
    return "";
  }

  @Override
  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> files)
      throws ReplicationException {
    return;
  }

  @Override
  public MetricsSource getSourceMetrics() {
    return metrics;
  }

  @Override
  public Map<String, ReplicationStatus> getWalGroupStatus() {
    return new HashMap<>();
  }
}

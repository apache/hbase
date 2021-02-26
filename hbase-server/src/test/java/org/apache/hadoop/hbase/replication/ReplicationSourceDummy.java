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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * Source that does nothing at all, helpful to test ReplicationSourceManager
 */
public class ReplicationSourceDummy implements ReplicationSourceInterface {

  ReplicationSourceManager manager;
  String peerClusterId;
  Path currentPath;
  MetricsSource metrics;
  WALFileLengthProvider walFileLengthProvider;
  AtomicBoolean startup = new AtomicBoolean(false);

  @Override
  public void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager,
      ReplicationQueueStorage rq, ReplicationPeer rp, Server server, String peerClusterId,
      UUID clusterId, WALFileLengthProvider walFileLengthProvider, MetricsSource metrics)
      throws IOException {
    this.manager = manager;
    this.peerClusterId = peerClusterId;
    this.metrics = metrics;
    this.walFileLengthProvider = walFileLengthProvider;
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
  public ReplicationSourceInterface startup() {
    startup.set(true);
    return this;
  }

  public boolean isStartup() {
    return startup.get();
  }

  @Override
  public void terminate(String reason) {
    terminate(reason, null);
  }

  @Override
  public void terminate(String reason, Exception e) {
    terminate(reason, e, true);
  }

  @Override
  public void terminate(String reason, Exception e, boolean clearMetrics) {
    if (clearMetrics) {
      this.metrics.clear();
    }
  }

  @Override
  public String getQueueId() {
    return peerClusterId;
  }

  @Override
  public String getPeerId() {
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
  public boolean isPeerEnabled() {
    return true;
  }

  @Override
  public boolean isSourceActive() {
    return true;
  }

  @Override
  public MetricsSource getSourceMetrics() {
    return metrics;
  }

  @Override
  public ReplicationEndpoint getReplicationEndpoint() {
    return null;
  }

  @Override
  public ReplicationSourceManager getSourceManager() {
    return manager;
  }

  @Override
  public void tryThrottle(int batchSize) throws InterruptedException {
  }

  @Override
  public void postShipEdits(List<Entry> entries, int batchSize) {
  }

  @Override
  public WALFileLengthProvider getWALFileLengthProvider() {
    return walFileLengthProvider;
  }

  @Override
  public ServerName getServerWALsBelongTo() {
    return null;
  }

  @Override
  public ReplicationQueueStorage getReplicationQueueStorage() {
    return null;
  }
}

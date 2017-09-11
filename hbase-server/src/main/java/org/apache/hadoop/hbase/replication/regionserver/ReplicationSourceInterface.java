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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * Interface that defines a replication source
 */
@InterfaceAudience.Private
public interface ReplicationSourceInterface {

  /**
   * Initializer for the source
   * @param conf the configuration to use
   * @param fs the file system to use
   * @param manager the manager to use
   * @param replicationQueues
   * @param replicationPeers
   * @param stopper the stopper object for this region server
   * @param peerClusterZnode
   * @param clusterId
   * @throws IOException
   */
  public void init(final Configuration conf, final FileSystem fs,
      final ReplicationSourceManager manager, final ReplicationQueues replicationQueues,
      final ReplicationPeers replicationPeers, final Stoppable stopper,
      final String peerClusterZnode, final UUID clusterId, ReplicationEndpoint replicationEndpoint,
      final MetricsSource metrics) throws IOException;

  /**
   * Add a log to the list of logs to replicate
   * @param log path to the log to replicate
   */
  void enqueueLog(Path log);

  /**
   * Add hfile names to the queue to be replicated.
   * @param tableName Name of the table these files belongs to
   * @param family Name of the family these files belong to
   * @param pairs list of pairs of { HFile location in staging dir, HFile path in region dir which
   *          will be added in the queue for replication}
   * @throws ReplicationException If failed to add hfile references
   */
  void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
      throws ReplicationException;

  /**
   * Start the replication
   */
  void startup();

  /**
   * End the replication
   * @param reason why it's terminating
   */
  void terminate(String reason);

  /**
   * End the replication
   * @param reason why it's terminating
   * @param cause the error that's causing it
   */
  void terminate(String reason, Exception cause);

  /**
   * Get the current log that's replicated
   * @return the current log
   */
  Path getCurrentPath();

  /**
   * Get the id that the source is replicating to
   *
   * @return peer cluster id
   */
  String getPeerClusterZnode();

  /**
   * Get the id that the source is replicating to.
   *
   * @return peer id
   */
  String getPeerId();

  /**
   * Get a string representation of the current statistics
   * for this source
   * @return printable stats
   */
  String getStats();

  /**
   * @return peer enabled or not
   */
  boolean isPeerEnabled();

  /**
   * @return active or not
   */
  boolean isSourceActive();

  /**
   * @return metrics of this replication source
   */
  MetricsSource getSourceMetrics();

  /**
   * @return the replication endpoint used by this replication source
   */
  ReplicationEndpoint getReplicationEndpoint();

  /**
   * @return the replication source manager
   */
  ReplicationSourceManager getSourceManager();

  /**
   * Try to throttle when the peer config with a bandwidth
   * @param batchSize entries size will be pushed
   * @throws InterruptedException
   */
  void tryThrottle(int batchSize) throws InterruptedException;

  /**
   * Call this after the shipper thread ship some entries to peer cluster.
   * @param entries pushed
   * @param batchSize entries size pushed
   */
  void postShipEdits(List<Entry> entries, int batchSize);
}

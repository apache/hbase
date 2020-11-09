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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationSourceController;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that defines a replication source
 */
@InterfaceAudience.Private
public interface ReplicationSourceInterface {

  /**
   * Initializer for the source
   *
   * @param conf configuration to use
   * @param fs file system to use
   * @param walDir the directory where the WAL is located
   * @param overallController the overall controller of all replication sources
   * @param queueStorage the replication queue storage
   * @param replicationPeer the replication peer
   * @param server the server which start and run this replication source
   * @param producer the name of region server which produce WAL to the replication queue
   * @param queueId the id of our replication queue
   * @param clusterId unique UUID for the cluster
   * @param walFileLengthProvider used to get the WAL length
   * @param metrics metrics for this replication source
   */
  void init(Configuration conf, FileSystem fs, Path walDir,
    ReplicationSourceController overallController, ReplicationQueueStorage queueStorage,
    ReplicationPeer replicationPeer, Server server, ServerName producer, String queueId,
    UUID clusterId, WALFileLengthProvider walFileLengthProvider, MetricsSource metrics)
    throws IOException;

  /**
   * Add a log to the list of logs to replicate
   * @param log path to the log to replicate
   */
  void enqueueLog(Path log);

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
   * End the replication
   * @param reason why it's terminating
   * @param cause the error that's causing it
   * @param clearMetrics removes all metrics about this Source
   */
  void terminate(String reason, Exception cause, boolean clearMetrics);

  /**
   * Get the current log that's replicated
   * @return the current log
   */
  Path getCurrentPath();

  /**
   * Get the queue id that the source is replicating to
   *
   * @return queue id
   */
  String getQueueId();

  /**
   * Get the id that the source is replicating to.
   * @return peer id
   */
  default String getPeerId() {
    return getPeer().getId();
  }

  /**
   * Get the replication peer instance.
   * @return the replication peer instance
   */
  ReplicationPeer getPeer();

  /**
   * Get a string representation of the current statistics
   * for this source
   * @return printable stats
   */
  String getStats();

  /**
   * @return peer enabled or not
   */
  default boolean isPeerEnabled() {
    return getPeer().isPeerEnabled();
  }

  /**
   * @return whether this is sync replication peer.
   */
  default boolean isSyncReplication() {
    return getPeer().getPeerConfig().isSyncReplication();
  }
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
   * @return the wal file length provider
   */
  WALFileLengthProvider getWALFileLengthProvider();

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

  /**
   * The queue of WALs only belong to one region server. This will return the server name which all
   * WALs belong to.
   * @return the server name which all WALs belong to
   */
  ServerName getServerWALsBelongTo();

  /**
   * get the stat of replication for each wal group.
   * @return stat of replication
   */
  default Map<String, ReplicationStatus> getWalGroupStatus() {
    return new HashMap<>();
  }

  /**
   * @return whether this is a replication source for recovery.
   */
  default boolean isRecovered() {
    return false;
  }

  /**
   * Set the current position of WAL to {@link ReplicationQueueStorage}
   * @param entryBatch a batch of WAL entries to replicate
   */
  void setWALPosition(WALEntryBatch entryBatch);

  /**
   * Cleans a WAL and all older WALs from replication queue. Called when we are sure that a WAL is
   * closed and has no more entries.
   * @param walName the name of WAL
   * @param inclusive whether we should also remove the given WAL
   */
  void cleanOldWALs(String walName, boolean inclusive);
}

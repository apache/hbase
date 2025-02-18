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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationResult;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that defines a replication source
 */
@InterfaceAudience.Private
public interface ReplicationSourceInterface {
  /**
   * Initializer for the source
   * @param conf                  the configuration to use
   * @param fs                    the file system to use
   * @param manager               the manager to use
   * @param queueStorage          the replication queue storage
   * @param replicationPeer       the replication peer
   * @param server                the server for this region server
   * @param queueData             the existing replication queue data, contains the queue id and
   *                              replication start offsets
   * @param clusterId             the cluster id
   * @param walFileLengthProvider for getting the length of the WAL file which is currently being
   *                              written
   * @param metrics               the replication metrics
   */
  void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager,
    ReplicationQueueStorage queueStorage, ReplicationPeer replicationPeer, Server server,
    ReplicationQueueData queueData, UUID clusterId, WALFileLengthProvider walFileLengthProvider,
    MetricsSource metrics) throws IOException;

  /**
   * Add a log to the list of logs to replicate
   * @param log path to the log to replicate
   */
  void enqueueLog(Path log);

  /**
   * Add hfile names to the queue to be replicated.
   * @param tableName Name of the table these files belongs to
   * @param family    Name of the family these files belong to
   * @param pairs     list of pairs of { HFile location in staging dir, HFile path in region dir
   *                  which will be added in the queue for replication}
   * @throws ReplicationException If failed to add hfile references
   */
  void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
    throws ReplicationException;

  /**
   * Start the replication
   */
  ReplicationSourceInterface startup();

  /**
   * End the replication
   * @param reason why it's terminating
   */
  void terminate(String reason);

  /**
   * End the replication
   * @param reason why it's terminating
   * @param cause  the error that's causing it
   */
  void terminate(String reason, Exception cause);

  /**
   * End the replication
   * @param reason       why it's terminating
   * @param cause        the error that's causing it
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
   * @return queue id
   */
  ReplicationQueueId getQueueId();

  /**
   * Get the id that the source is replicating to.
   * @return peer id
   */
  default String getPeerId() {
    return getQueueId().getPeerId();
  }

  /**
   * Get the replication peer instance.
   * @return the replication peer instance
   */
  ReplicationPeer getPeer();

  /**
   * Get a string representation of the current statistics for this source
   * @return printable stats
   */
  String getStats();

  /** Returns peer enabled or not */
  default boolean isPeerEnabled() {
    return getPeer().isPeerEnabled();
  }

  /** Returns whether this is sync replication peer. */
  default boolean isSyncReplication() {
    return getPeer().getPeerConfig().isSyncReplication();
  }

  /** Returns active or not */
  boolean isSourceActive();

  /** Returns metrics of this replication source */
  MetricsSource getSourceMetrics();

  /** Returns the replication endpoint used by this replication source */
  ReplicationEndpoint getReplicationEndpoint();

  /** Returns the replication source manager */
  ReplicationSourceManager getSourceManager();

  /** Returns the wal file length provider */
  WALFileLengthProvider getWALFileLengthProvider();

  /**
   * Try to throttle when the peer config with a bandwidth
   * @param batchSize entries size will be pushed
   */
  void tryThrottle(int batchSize) throws InterruptedException;

  /**
   * Call this after the shipper thread ship some entries to peer cluster.
   * @param entries   pushed
   * @param batchSize entries size pushed
   */
  void postShipEdits(List<Entry> entries, long batchSize);

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

  /** Returns whether this is a replication source for recovery. */
  default boolean isRecovered() {
    return getQueueId().isRecovered();
  }

  /** Returns The instance of queueStorage used by this ReplicationSource. */
  ReplicationQueueStorage getReplicationQueueStorage();

  /**
   * Log the current position to storage. Also clean old logs from the replication queue. Use to
   * bypass the default call to
   * {@link ReplicationSourceManager#logPositionAndCleanOldLogs(ReplicationSourceInterface, WALEntryBatch)}
   * whem implementation does not need to persist state to backing storage.
   * @param entryBatch the wal entry batch we just shipped
   * @return The instance of queueStorage used by this ReplicationSource.
   */
  default void logPositionAndCleanOldLogs(WALEntryBatch entryBatch, ReplicationResult replicated) {

  }

  default public void persistOffsets() {

  }
}

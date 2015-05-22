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

package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;

import com.google.common.util.concurrent.Service;

/**
 * ReplicationEndpoint is a plugin which implements replication
 * to other HBase clusters, or other systems. ReplicationEndpoint implementation
 * can be specified at the peer creation time by specifying it
 * in the {@link ReplicationPeerConfig}. A ReplicationEndpoint is run in a thread
 * in each region server in the same process.
 * <p>
 * ReplicationEndpoint is closely tied to ReplicationSource in a producer-consumer
 * relation. ReplicationSource is an HBase-private class which tails the logs and manages
 * the queue of logs plus management and persistence of all the state for replication.
 * ReplicationEndpoint on the other hand is responsible for doing the actual shipping
 * and persisting of the WAL entries in the other cluster.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public interface ReplicationEndpoint extends Service {

  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
  class Context {
    private final Configuration conf;
    private final FileSystem fs;
    private final TableDescriptors tableDescriptors;
    private final ReplicationPeerConfig peerConfig;
    private final ReplicationPeer replicationPeer;
    private final String peerId;
    private final UUID clusterId;
    private final MetricsSource metrics;

    @InterfaceAudience.Private
    public Context(
        final Configuration conf,
        final FileSystem fs,
        final ReplicationPeerConfig peerConfig,
        final String peerId,
        final UUID clusterId,
        final ReplicationPeer replicationPeer,
        final MetricsSource metrics,
        final TableDescriptors tableDescriptors) {
      this.peerConfig = peerConfig;
      this.conf = conf;
      this.fs = fs;
      this.clusterId = clusterId;
      this.peerId = peerId;
      this.replicationPeer = replicationPeer;
      this.metrics = metrics;
      this.tableDescriptors = tableDescriptors;
    }
    public Configuration getConfiguration() {
      return conf;
    }
    public FileSystem getFilesystem() {
      return fs;
    }
    public UUID getClusterId() {
      return clusterId;
    }
    public String getPeerId() {
      return peerId;
    }
    public ReplicationPeerConfig getPeerConfig() {
      return peerConfig;
    }
    public ReplicationPeer getReplicationPeer() {
      return replicationPeer;
    }
    public MetricsSource getMetrics() {
      return metrics;
    }
    public TableDescriptors getTableDescriptors() {
      return tableDescriptors;
    }
  }

  /**
   * Initialize the replication endpoint with the given context.
   * @param context replication context
   * @throws IOException
   */
  void init(Context context) throws IOException;

  /** Whether or not, the replication endpoint can replicate to it's source cluster with the same
   * UUID */
  boolean canReplicateToSameCluster();

  /**
   * Returns a UUID of the provided peer id. Every HBase cluster instance has a persisted
   * associated UUID. If the replication is not performed to an actual HBase cluster (but
   * some other system), the UUID returned has to uniquely identify the connected target system.
   * @return a UUID or null if the peer cluster does not exist or is not connected.
   */
  UUID getPeerUUID();

  /**
   * Returns a WALEntryFilter to use for filtering out WALEntries from the log. Replication
   * infrastructure will call this filter before sending the edits to shipEdits().
   * @return a {@link WALEntryFilter} or null.
   */
  WALEntryFilter getWALEntryfilter();

  /**
   * A context for {@link ReplicationEndpoint#replicate(ReplicateContext)} method.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
  static class ReplicateContext {
    List<Entry> entries;
    int size;
    @InterfaceAudience.Private
    public ReplicateContext() {
    }

    public ReplicateContext setEntries(List<Entry> entries) {
      this.entries = entries;
      return this;
    }
    public ReplicateContext setSize(int size) {
      this.size = size;
      return this;
    }
    public List<Entry> getEntries() {
      return entries;
    }
    public int getSize() {
      return size;
    }
  }

  /**
   * Replicate the given set of entries (in the context) to the other cluster.
   * Can block until all the given entries are replicated. Upon this method is returned,
   * all entries that were passed in the context are assumed to be persisted in the
   * target cluster.
   * @param replicateContext a context where WAL entries and other
   * parameters can be obtained.
   */
  boolean replicate(ReplicateContext replicateContext);

}

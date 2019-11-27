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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.Abortable;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;

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
public interface ReplicationEndpoint extends ReplicationPeerConfigListener {
  // TODO: This class needs doc. Has a Context and a ReplicationContext. Then has #start, #stop.
  // How they relate? Do we #start before #init(Context)? We fail fast if you don't?

  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
  class Context {
    private final Configuration localConf;
    private final Configuration conf;
    private final FileSystem fs;
    private final TableDescriptors tableDescriptors;
    private final ReplicationPeer replicationPeer;
    private final String peerId;
    private final UUID clusterId;
    private final MetricsSource metrics;
    private final Abortable abortable;

    @InterfaceAudience.Private
    public Context(
        final Configuration localConf,
        final Configuration conf,
        final FileSystem fs,
        final String peerId,
        final UUID clusterId,
        final ReplicationPeer replicationPeer,
        final MetricsSource metrics,
        final TableDescriptors tableDescriptors,
        final Abortable abortable) {
      this.localConf = localConf;
      this.conf = conf;
      this.fs = fs;
      this.clusterId = clusterId;
      this.peerId = peerId;
      this.replicationPeer = replicationPeer;
      this.metrics = metrics;
      this.tableDescriptors = tableDescriptors;
      this.abortable = abortable;
    }
    public Configuration getConfiguration() {
      return conf;
    }
    public Configuration getLocalConfiguration() {
      return localConf;
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
      return replicationPeer.getPeerConfig();
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
    public Abortable getAbortable() { return abortable; }
  }

  /**
   * Initialize the replication endpoint with the given context.
   * @param context replication context
   * @throws IOException error occur when initialize the endpoint.
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
    String walGroupId;
    int timeout;
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
    public ReplicateContext setWalGroupId(String walGroupId) {
      this.walGroupId = walGroupId;
      return this;
    }
    public List<Entry> getEntries() {
      return entries;
    }
    public int getSize() {
      return size;
    }
    public String getWalGroupId(){
      return walGroupId;
    }
    public void setTimeout(int timeout) {
      this.timeout = timeout;
    }
    public int getTimeout() {
      return this.timeout;
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


  // The below methods are inspired by Guava Service. See
  // https://github.com/google/guava/wiki/ServiceExplained for overview of Guava Service.
  // Below we implement a subset only with different names on some methods so we can implement
  // the below internally using Guava (without exposing our implementation to
  // ReplicationEndpoint implementors.

  /**
   * Returns {@code true} if this service is RUNNING.
   */
  boolean isRunning();

  /**
   * @return Return {@code true} is this service is STARTING (but not yet RUNNING).
   */
  boolean isStarting();

  /**
   * Initiates service startup and returns immediately. A stopped service may not be restarted.
   * Equivalent of startAsync call in Guava Service.
   * @throws IllegalStateException if the service is not new, if it has been run already.
   */
  void start();

  /**
   * Waits for the {@link ReplicationEndpoint} to be up and running.
   *
   * @throws IllegalStateException if the service reaches a state from which it is not possible to
   *     enter the (internal) running state. e.g. if the state is terminated when this method is
   *     called then this will throw an IllegalStateException.
   */
  void awaitRunning();

  /**
   * Waits for the {@link ReplicationEndpoint} to to be up and running for no more
   * than the given time.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @throws TimeoutException if the service has not reached the given state within the deadline
   * @throws IllegalStateException if the service reaches a state from which it is not possible to
   *     enter the (internal) running state. e.g. if the state is terminated when this method is
   *     called then this will throw an IllegalStateException.
   */
  void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * If the service is starting or running, this initiates service shutdown and returns immediately.
   * If the service has already been stopped, this method returns immediately without taking action.
   * Equivalent of stopAsync call in Guava Service.
   */
  void stop();

  /**
   * Waits for the {@link ReplicationEndpoint} to reach the terminated (internal) state.
   *
   * @throws IllegalStateException if the service FAILED.
   */
  void awaitTerminated();

  /**
   * Waits for the {@link ReplicationEndpoint} to reach a terminal state for no
   * more than the given time.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @throws TimeoutException if the service has not reached the given state within the deadline
   * @throws IllegalStateException if the service FAILED.
   */
  void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Returns the {@link Throwable} that caused this service to fail.
   *
   * @throws IllegalStateException if this service's state isn't FAILED.
   */
  Throwable failureCause();
}

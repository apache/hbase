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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.AsyncReplicationServerAdmin;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * Maintains a collection of peers to replicate to, and randomly selects a
 * single peer to replicate to per set of data to replicate. Also handles
 * keeping track of peer availability.
 */
@InterfaceAudience.Private
public class ReplicationSinkManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSinkManager.class);

  /**
   * Default maximum number of times a replication sink can be reported as bad before
   * it will no longer be provided as a sink for replication without the pool of
   * replication sinks being refreshed.
   */
  static final int DEFAULT_BAD_SINK_THRESHOLD = 3;

  /**
   * Default ratio of the total number of peer cluster region servers to consider
   * replicating to.
   */
  static final float DEFAULT_REPLICATION_SOURCE_RATIO = 0.5f;


  private final AsyncClusterConnection conn;

  private final HBaseReplicationEndpoint endpoint;

  // Count of "bad replication sink" reports per peer sink
  private final Map<ServerName, Integer> badReportCounts;

  // Ratio of total number of potential peer region servers to be used
  private final float ratio;

  // Maximum number of times a sink can be reported as bad before the pool of
  // replication sinks is refreshed
  private final int badSinkThreshold;

  // A timestamp of the last time the list of replication peers changed
  private long lastUpdateToPeers;

  // The current pool of sinks to which replication can be performed
  private List<ServerName> sinks = Lists.newArrayList();

  /**
   * Instantiate for a single replication peer cluster.
   * @param conn connection to the peer cluster
   * @param endpoint replication endpoint for inter cluster replication
   * @param conf HBase configuration, used for determining replication source ratio and bad peer
   *          threshold
   */
  public ReplicationSinkManager(AsyncClusterConnection conn, HBaseReplicationEndpoint endpoint,
      Configuration conf) {
    this.conn = conn;
    this.endpoint = endpoint;
    this.badReportCounts = Maps.newHashMap();
    this.ratio = conf.getFloat("replication.source.ratio", DEFAULT_REPLICATION_SOURCE_RATIO);
    this.badSinkThreshold =
      conf.getInt("replication.bad.sink.threshold", DEFAULT_BAD_SINK_THRESHOLD);
  }

  /**
   * Get a randomly-chosen replication sink to replicate to.
   * @return a replication sink to replicate to
   */
  public synchronized SinkPeer getReplicationSink() throws IOException {
    if (endpoint.getLastRegionServerUpdate() > this.lastUpdateToPeers || sinks.isEmpty()) {
      LOG.info("Current list of sinks is out of date or empty, updating");
      chooseSinks();
    }

    if (sinks.isEmpty()) {
      throw new IOException("No replication sinks are available");
    }
    ServerName serverName = sinks.get(ThreadLocalRandom.current().nextInt(sinks.size()));
    return createSinkPeer(serverName);
  }

  /**
   * Report a {@code SinkPeer} as being bad (i.e. an attempt to replicate to it
   * failed). If a single SinkPeer is reported as bad more than
   * replication.bad.sink.threshold times, it will be removed
   * from the pool of potential replication targets.
   *
   * @param sinkPeer
   *          The SinkPeer that had a failed replication attempt on it
   */
  public synchronized void reportBadSink(SinkPeer sinkPeer) {
    ServerName serverName = sinkPeer.getServerName();
    int badReportCount = (badReportCounts.containsKey(serverName)
                    ? badReportCounts.get(serverName) : 0) + 1;
    badReportCounts.put(serverName, badReportCount);
    if (badReportCount > badSinkThreshold) {
      this.sinks.remove(serverName);
      if (sinks.isEmpty()) {
        chooseSinks();
      }
    }
  }

  /**
   * Report that a {@code SinkPeer} successfully replicated a chunk of data.
   *
   * @param sinkPeer
   *          The SinkPeer that had a failed replication attempt on it
   */
  public synchronized void reportSinkSuccess(SinkPeer sinkPeer) {
    badReportCounts.remove(sinkPeer.getServerName());
  }

  /**
   * Refresh the list of sinks.
   */
  public synchronized void chooseSinks() {
    List<ServerName> slaveAddresses = endpoint.getRegionServers();
    if(slaveAddresses.isEmpty()){
      LOG.warn("No sinks available at peer. Will not be able to replicate");
    }
    Collections.shuffle(slaveAddresses, ThreadLocalRandom.current());
    int numSinks = (int) Math.ceil(slaveAddresses.size() * ratio);
    sinks = slaveAddresses.subList(0, numSinks);
    lastUpdateToPeers = System.currentTimeMillis();
    badReportCounts.clear();
  }

  public synchronized int getNumSinks() {
    return sinks.size();
  }

  @VisibleForTesting
  protected List<ServerName> getSinksForTesting() {
    return Collections.unmodifiableList(sinks);
  }

  /**
   * Wraps a replication region server sink to provide the ability to identify
   * it.
   */
  public static abstract class SinkPeer {
    private ServerName serverName;

    public SinkPeer(ServerName serverName) {
      this.serverName = serverName;
    }

    ServerName getServerName() {
      return serverName;
    }

    public abstract void replicateWALEntry(WAL.Entry[] entries, String replicationClusterId,
        Path sourceBaseNamespaceDir, Path sourceHFileArchiveDir, int timeout) throws IOException;
  }

  public static class RegionServerSinkPeer extends SinkPeer {

    private AsyncRegionServerAdmin regionServer;

    public RegionServerSinkPeer(ServerName serverName,
        AsyncRegionServerAdmin replicationServer) {
      super(serverName);
      this.regionServer = replicationServer;
    }

    public void replicateWALEntry(WAL.Entry[] entries, String replicationClusterId,
        Path sourceBaseNamespaceDir, Path sourceHFileArchiveDir, int timeout) throws IOException {
      ReplicationProtbufUtil.replicateWALEntry(regionServer, entries, replicationClusterId,
          sourceBaseNamespaceDir, sourceHFileArchiveDir, timeout);
    }
  }

  public static class ReplicationServerSinkPeer extends SinkPeer {

    private AsyncReplicationServerAdmin replicationServer;

    public ReplicationServerSinkPeer(ServerName serverName,
        AsyncReplicationServerAdmin replicationServer) {
      super(serverName);
      this.replicationServer = replicationServer;
    }

    public void replicateWALEntry(WAL.Entry[] entries, String replicationClusterId,
        Path sourceBaseNamespaceDir, Path sourceHFileArchiveDir, int timeout) throws IOException {
      ReplicationProtbufUtil.replicateWALEntry(replicationServer, entries, replicationClusterId,
          sourceBaseNamespaceDir, sourceHFileArchiveDir, timeout);
    }
  }

  private SinkPeer createSinkPeer(ServerName serverName) throws IOException {
    if (ReplicationUtils.isPeerClusterSupportReplicationOffload(conn)) {
      return new ReplicationServerSinkPeer(serverName, conn.getReplicationServerAdmin(serverName));
    } else {
      return new RegionServerSinkPeer(serverName, conn.getRegionServerAdmin(serverName));
    }
  }
}

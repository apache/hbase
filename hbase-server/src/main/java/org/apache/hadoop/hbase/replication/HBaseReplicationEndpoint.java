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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.ReservoirSample;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * A {@link BaseReplicationEndpoint} for replication endpoints whose target cluster is an HBase
 * cluster.
 */
@InterfaceAudience.Private
public abstract class HBaseReplicationEndpoint extends BaseReplicationEndpoint
  implements Abortable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseReplicationEndpoint.class);

  protected Configuration conf;

  private final Object connLock = new Object();

  private volatile AsyncClusterConnection conn;

  /**
   * Default maximum number of times a replication sink can be reported as bad before it will no
   * longer be provided as a sink for replication without the pool of replication sinks being
   * refreshed.
   */
  public static final int DEFAULT_BAD_SINK_THRESHOLD = 3;

  /**
   * Default ratio of the total number of peer cluster region servers to consider replicating to.
   */
  public static final float DEFAULT_REPLICATION_SOURCE_RATIO = 0.5f;

  // Ratio of total number of potential peer region servers to be used
  private float ratio;

  // Maximum number of times a sink can be reported as bad before the pool of
  // replication sinks is refreshed
  private int badSinkThreshold;
  // Count of "bad replication sink" reports per peer sink
  private Map<ServerName, Integer> badReportCounts;

  private List<ServerName> sinkServers = new ArrayList<>(0);

  /*
   * Some implementations of HBaseInterClusterReplicationEndpoint may require instantiate different
   * Connection implementations, or initialize it in a different way, so defining createConnection
   * as protected for possible overridings.
   */
  protected AsyncClusterConnection createConnection(Configuration conf) throws IOException {
    return ClusterConnectionFactory.createAsyncClusterConnection(conf, null, User.getCurrent());
  }

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.conf = HBaseConfiguration.create(ctx.getConfiguration());
    this.ratio =
      ctx.getConfiguration().getFloat("replication.source.ratio", DEFAULT_REPLICATION_SOURCE_RATIO);
    this.badSinkThreshold =
      ctx.getConfiguration().getInt("replication.bad.sink.threshold", DEFAULT_BAD_SINK_THRESHOLD);
    this.badReportCounts = Maps.newHashMap();
  }

  private void disconnect() {
    synchronized (connLock) {
      if (this.conn != null) {
        try {
          this.conn.close();
          this.conn = null;
        } catch (IOException e) {
          LOG.warn("{} Failed to close the connection", ctx.getPeerId());
        }
      }
    }
  }

  @Override
  public void start() {
    startAsync();
  }

  @Override
  public void stop() {
    stopAsync();
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    disconnect();
    notifyStopped();
  }

  @Override
  public UUID getPeerUUID() {
    try {
      AsyncClusterConnection conn = connect();
      String clusterId = FutureUtils
        .get(conn.getAdmin().getClusterMetrics(EnumSet.of(ClusterMetrics.Option.CLUSTER_ID)))
        .getClusterId();
      return UUID.fromString(clusterId);
    } catch (IOException e) {
      LOG.warn("Failed to get cluster id for cluster", e);
      return null;
    }
  }

  // do not call this method in doStart method, only initialize the connection to remote cluster
  // when you actually wants to make use of it. The problem here is that, starting the replication
  // endpoint is part of the region server initialization work, so if the peer cluster is fully
  // down and we can not connect to it, we will cause the initialization to fail and crash the
  // region server, as we need the cluster id while setting up the AsyncClusterConnection, which
  // needs to at least connect to zookeeper or some other servers in the peer cluster based on
  // different connection registry implementation
  private AsyncClusterConnection connect() throws IOException {
    AsyncClusterConnection c = this.conn;
    if (c != null) {
      return c;
    }
    synchronized (connLock) {
      c = this.conn;
      if (c != null) {
        return c;
      }
      c = createConnection(this.conf);
      conn = c;
    }
    return c;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.error("The HBaseReplicationEndpoint corresponding to peer " + ctx.getPeerId()
      + " was aborted for the following reason(s):" + why, e);
  }

  @Override
  public boolean isAborted() {
    // Currently this is never "Aborted", we just log when the abort method is called.
    return false;
  }

  /**
   * Get the list of all the region servers from the specified peer
   * @return list of region server addresses or an empty list if the slave is unavailable
   */
  // will be overrided in tests so protected
  protected Collection<ServerName> fetchPeerAddresses() {
    try {
      return FutureUtils.get(connect().getAdmin().getRegionServers(true));
    } catch (IOException e) {
      LOG.debug("Fetch peer addresses failed", e);
      return Collections.emptyList();
    }
  }

  protected synchronized void chooseSinks() {
    Collection<ServerName> slaveAddresses = fetchPeerAddresses();
    if (slaveAddresses.isEmpty()) {
      LOG.warn("No sinks available at peer. Will not be able to replicate");
      this.sinkServers = Collections.emptyList();
    } else {
      int numSinks = (int) Math.ceil(slaveAddresses.size() * ratio);
      ReservoirSample<ServerName> sample = new ReservoirSample<>(numSinks);
      sample.add(slaveAddresses.iterator());
      this.sinkServers = sample.getSamplingResult();
    }
    badReportCounts.clear();
  }

  protected synchronized int getNumSinks() {
    return sinkServers.size();
  }

  /**
   * Get a randomly-chosen replication sink to replicate to.
   * @return a replication sink to replicate to
   */
  protected synchronized SinkPeer getReplicationSink() throws IOException {
    if (sinkServers.isEmpty()) {
      LOG.info("Current list of sinks is out of date or empty, updating");
      chooseSinks();
    }
    if (sinkServers.isEmpty()) {
      throw new IOException("No replication sinks are available");
    }
    ServerName serverName =
      sinkServers.get(ThreadLocalRandom.current().nextInt(sinkServers.size()));
    return new SinkPeer(serverName, connect().getRegionServerAdmin(serverName));
  }

  /**
   * Report a {@code SinkPeer} as being bad (i.e. an attempt to replicate to it failed). If a single
   * SinkPeer is reported as bad more than replication.bad.sink.threshold times, it will be removed
   * from the pool of potential replication targets.
   * @param sinkPeer The SinkPeer that had a failed replication attempt on it
   */
  protected synchronized void reportBadSink(SinkPeer sinkPeer) {
    ServerName serverName = sinkPeer.getServerName();
    int badReportCount = badReportCounts.compute(serverName, (k, v) -> v == null ? 1 : v + 1);
    if (badReportCount > badSinkThreshold) {
      this.sinkServers.remove(serverName);
      if (sinkServers.isEmpty()) {
        chooseSinks();
      }
    }
  }

  /**
   * Report that a {@code SinkPeer} successfully replicated a chunk of data. The SinkPeer that had a
   * failed replication attempt on it
   */
  protected synchronized void reportSinkSuccess(SinkPeer sinkPeer) {
    badReportCounts.remove(sinkPeer.getServerName());
  }

  List<ServerName> getSinkServers() {
    return sinkServers;
  }

  /**
   * Wraps a replication region server sink to provide the ability to identify it.
   */
  public static class SinkPeer {
    private ServerName serverName;
    private AsyncRegionServerAdmin regionServer;

    public SinkPeer(ServerName serverName, AsyncRegionServerAdmin regionServer) {
      this.serverName = serverName;
      this.regionServer = regionServer;
    }

    ServerName getServerName() {
      return serverName;
    }

    public AsyncRegionServerAdmin getRegionServer() {
      return regionServer;
    }
  }
}

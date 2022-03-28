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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseRpcServicesBase;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounter.ExponentialBackoffPolicyWithLimit;
import org.apache.hadoop.hbase.util.RetryCounter.RetryConfig;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manage the bootstrap node list at region server side.
 * <p/>
 * It will request master first to get the initial set of bootstrap nodes(a sub set of live region
 * servers), and then it will exchange the bootstrap nodes with other bootstrap nodes. In most
 * cases, if the cluster is stable, we do not need to request master again until we reach the
 * request master interval. And if the current number of bootstrap nodes is not enough, we will
 * request master soon.
 * <p/>
 * The algorithm is very simple, as we will always fallback to request master. THe trick here is
 * that, if we can not get enough bootstrap nodes from master, then the cluster will be small, so it
 * will not put too much pressure on master if we always request master. And for large clusters, we
 * will soon get enough bootstrap nodes and stop requesting master.
 */
@InterfaceAudience.Private
public class BootstrapNodeManager {

  private static final Logger LOG = LoggerFactory.getLogger(BootstrapNodeManager.class);

  public static final String REQUEST_MASTER_INTERVAL_SECS =
    "hbase.server.bootstrap.request_master_interval.secs";

  // default request every 10 minutes
  public static final long DEFAULT_REQUEST_MASTER_INTERVAL_SECS = TimeUnit.MINUTES.toSeconds(10);

  public static final String REQUEST_MASTER_MIN_INTERVAL_SECS =
    "hbase.server.bootstrap.request_master_min_interval.secs";

  // default 30 seconds
  public static final long DEFAULT_REQUEST_MASTER_MIN_INTERVAL_SECS = 30;

  public static final String REQUEST_REGIONSERVER_INTERVAL_SECS =
    "hbase.server.bootstrap.request_regionserver_interval.secs";

  // default request every 30 seconds
  public static final long DEFAULT_REQUEST_REGIONSERVER_INTERVAL_SECS = 30;

  private static final float JITTER = 0.2f;

  private volatile List<ServerName> nodes = Collections.emptyList();

  private final AsyncClusterConnection conn;

  private final MasterAddressTracker masterAddrTracker;

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(getClass().getSimpleName()).build());

  private final long requestMasterIntervalSecs;

  private final long requestMasterMinIntervalSecs;

  private final long requestRegionServerIntervalSecs;

  private final int maxNodeCount;

  private final RetryCounterFactory retryCounterFactory;

  private RetryCounter retryCounter;

  private long lastRequestMasterTime;

  public BootstrapNodeManager(AsyncClusterConnection conn, MasterAddressTracker masterAddrTracker) {
    this.conn = conn;
    this.masterAddrTracker = masterAddrTracker;
    Configuration conf = conn.getConfiguration();
    requestMasterIntervalSecs =
      conf.getLong(REQUEST_MASTER_INTERVAL_SECS, DEFAULT_REQUEST_MASTER_INTERVAL_SECS);
    requestMasterMinIntervalSecs =
      conf.getLong(REQUEST_MASTER_MIN_INTERVAL_SECS, DEFAULT_REQUEST_MASTER_MIN_INTERVAL_SECS);
    requestRegionServerIntervalSecs =
      conf.getLong(REQUEST_REGIONSERVER_INTERVAL_SECS, DEFAULT_REQUEST_REGIONSERVER_INTERVAL_SECS);
    maxNodeCount = conf.getInt(HBaseRpcServicesBase.CLIENT_BOOTSTRAP_NODE_LIMIT,
      HBaseRpcServicesBase.DEFAULT_CLIENT_BOOTSTRAP_NODE_LIMIT);
    retryCounterFactory = new RetryCounterFactory(
      new RetryConfig().setBackoffPolicy(new ExponentialBackoffPolicyWithLimit()).setJitter(JITTER)
        .setSleepInterval(requestMasterMinIntervalSecs).setMaxSleepTime(requestMasterIntervalSecs)
        .setTimeUnit(TimeUnit.SECONDS));
    executor.schedule(this::getFromMaster, getDelay(requestMasterMinIntervalSecs),
      TimeUnit.SECONDS);
  }

  private long getDelay(long delay) {
    long jitterDelay = (long) (delay * ThreadLocalRandom.current().nextFloat() * JITTER);
    return delay + jitterDelay;
  }

  private void getFromMaster() {
    List<ServerName> liveRegionServers;
    try {
      // get 2 times number of node
      liveRegionServers =
        FutureUtils.get(conn.getLiveRegionServers(masterAddrTracker, maxNodeCount * 2));
    } catch (IOException e) {
      LOG.warn("failed to get live region servers from master", e);
      if (retryCounter == null) {
        retryCounter = retryCounterFactory.create();
      }
      executor.schedule(this::getFromMaster, retryCounter.getBackoffTimeAndIncrementAttempts(),
        TimeUnit.SECONDS);
      return;
    }
    retryCounter = null;
    lastRequestMasterTime = EnvironmentEdgeManager.currentTime();
    this.nodes = Collections.unmodifiableList(liveRegionServers);
    if (liveRegionServers.size() < maxNodeCount) {
      // If the number of live region servers is small, it means the cluster is small, so requesting
      // master with a higher frequency will not be a big problem, so here we will always request
      // master to get the live region servers as bootstrap nodes.
      executor.schedule(this::getFromMaster, getDelay(requestMasterMinIntervalSecs),
        TimeUnit.SECONDS);
      return;
    }
    // schedule tasks to exchange the bootstrap nodes with other region servers.
    executor.schedule(this::getFromRegionServer, getDelay(requestRegionServerIntervalSecs),
      TimeUnit.SECONDS);
  }

  // this method is also used to test whether a given region server is still alive.
  private void getFromRegionServer() {
    if (EnvironmentEdgeManager.currentTime() - lastRequestMasterTime >= TimeUnit.SECONDS
      .toMillis(requestMasterIntervalSecs)) {
      // schedule a get from master task immediately if haven't request master for more than
      // requestMasterIntervalSecs
      executor.execute(this::getFromMaster);
      return;
    }
    List<ServerName> currentList = this.nodes;
    ServerName peer = currentList.get(ThreadLocalRandom.current().nextInt(currentList.size()));
    List<ServerName> otherList;
    try {
      otherList = FutureUtils.get(conn.getAllBootstrapNodes(peer));
    } catch (IOException e) {
      LOG.warn("failed to request region server {}", peer, e);
      // remove this region server from the list since it can not respond successfully
      List<ServerName> newList = currentList.stream().filter(sn -> sn != peer)
        .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
      this.nodes = newList;
      if (newList.size() < maxNodeCount) {
        // schedule a get from master task immediately
        executor.execute(this::getFromMaster);
      } else {
        executor.schedule(this::getFromRegionServer, getDelay(requestRegionServerIntervalSecs),
          TimeUnit.SECONDS);
      }
      return;
    }
    // randomly select new live region server list
    Set<ServerName> newRegionServers = new HashSet<ServerName>(currentList);
    newRegionServers.addAll(otherList);
    List<ServerName> newList = new ArrayList<ServerName>(newRegionServers);
    Collections.shuffle(newList, ThreadLocalRandom.current());
    int expectedListSize = maxNodeCount * 2;
    if (newList.size() <= expectedListSize) {
      this.nodes = Collections.unmodifiableList(newList);
    } else {
      this.nodes =
        Collections.unmodifiableList(new ArrayList<>(newList.subList(0, expectedListSize)));
    }
    // schedule a new get from region server task
    executor.schedule(this::getFromRegionServer, requestRegionServerIntervalSecs, TimeUnit.SECONDS);
  }

  public void stop() {
    executor.shutdownNow();
  }

  public List<ServerName> getBootstrapNodes() {
    return nodes;
  }
}

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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionServerInfo;

/**
 * Tracks the online region servers via ZK.
 * <p/>
 * Handling of new RSs checking in is done via RPC. This class is only responsible for watching for
 * expired nodes. It handles listening for changes in the RS node list. The only exception is when
 * master restart, we will use the list fetched from zk to construct the initial set of live region
 * servers.
 * <p/>
 * If an RS node gets deleted, this automatically handles calling of
 * {@link ServerManager#expireServer(ServerName)}
 */
@InterfaceAudience.Private
public class RegionServerTracker extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerTracker.class);
  // indicate whether we are active master
  private boolean active;
  private volatile Set<ServerName> regionServers = Collections.emptySet();
  private final MasterServices server;
  // As we need to send request to zk when processing the nodeChildrenChanged event, we'd better
  // move the operation to a single threaded thread pool in order to not block the zk event
  // processing since all the zk listener across HMaster will be called in one thread sequentially.
  private final ExecutorService executor;

  public RegionServerTracker(ZKWatcher watcher, MasterServices server) {
    super(watcher);
    this.server = server;
    this.executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("RegionServerTracker-%d").build());
    watcher.registerListener(this);
    refresh();
  }

  private RegionServerInfo getServerInfo(ServerName serverName)
    throws KeeperException, IOException {
    String nodePath = watcher.getZNodePaths().getRsPath(serverName);
    byte[] data;
    try {
      data = ZKUtil.getData(watcher, nodePath);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
    if (data == null) {
      // we should receive a children changed event later and then we will expire it, so we still
      // need to add it to the region server set.
      LOG.warn("Server node {} does not exist, already dead?", serverName);
      return null;
    }
    if (data.length == 0 || !ProtobufUtil.isPBMagicPrefix(data)) {
      // this should not happen actually, unless we have bugs or someone has messed zk up.
      LOG.warn("Invalid data for region server node {} on zookeeper, data length = {}", serverName,
        data.length);
      return null;
    }
    RegionServerInfo.Builder builder = RegionServerInfo.newBuilder();
    int magicLen = ProtobufUtil.lengthOfPBMagic();
    ProtobufUtil.mergeFrom(builder, data, magicLen, data.length - magicLen);
    return builder.build();
  }

  /**
   * Upgrade to active master mode, where besides tracking the changes of region server set, we will
   * also started to add new region servers to ServerManager and also schedule SCP if a region
   * server dies. Starts the tracking of online RegionServers. All RSes will be tracked after this
   * method is called.
   * <p/>
   * In this method, we will also construct the region server sets in {@link ServerManager}. If a
   * region server is dead between the crash of the previous master instance and the start of the
   * current master instance, we will schedule a SCP for it. This is done in
   * {@link ServerManager#findDeadServersAndProcess(Set, Set)}, we call it here under the lock
   * protection to prevent concurrency issues with server expiration operation.
   * @param deadServersFromPE the region servers which already have SCP associated.
   * @param liveServersBeforeRestart the live region servers we recorded before master restarts.
   * @param splittingServersFromWALDir Servers whose WALs are being actively 'split'.
   */
  public void upgrade(Set<ServerName> deadServersFromPE, Set<ServerName> liveServersBeforeRestart,
    Set<ServerName> splittingServersFromWALDir) throws KeeperException, IOException {
    LOG.info(
      "Upgrading RegionServerTracker to active master mode; {} have existing" +
        "ServerCrashProcedures, {} possibly 'live' servers, and {} 'splitting'.",
      deadServersFromPE.size(), liveServersBeforeRestart.size(), splittingServersFromWALDir.size());
    // deadServersFromPE is made from a list of outstanding ServerCrashProcedures.
    // splittingServersFromWALDir are being actively split -- the directory in the FS ends in
    // '-SPLITTING'. Each splitting server should have a corresponding SCP. Log if not.
    splittingServersFromWALDir.stream().filter(s -> !deadServersFromPE.contains(s)).
      forEach(s -> LOG.error("{} has no matching ServerCrashProcedure", s));
    // create ServerNode for all possible live servers from wal directory
    liveServersBeforeRestart
        .forEach(sn -> server.getAssignmentManager().getRegionStates().getOrCreateServer(sn));
    ServerManager serverManager = server.getServerManager();
    synchronized (this) {
      Set<ServerName> liveServers = regionServers;
      for (ServerName serverName : liveServers) {
        RegionServerInfo info = getServerInfo(serverName);
        ServerMetrics serverMetrics = info != null ? ServerMetricsBuilder.of(serverName,
          VersionInfoUtil.getVersionNumber(info.getVersionInfo()),
          info.getVersionInfo().getVersion()) : ServerMetricsBuilder.of(serverName);
        serverManager.checkAndRecordNewServer(serverName, serverMetrics);
      }
      serverManager.findDeadServersAndProcess(deadServersFromPE, liveServersBeforeRestart);
      active = true;
    }
  }

  public void stop() {
    executor.shutdownNow();
  }

  public Set<ServerName> getRegionServers() {
    return regionServers;
  }

  // execute the operations which are only needed for active masters, such as expire old servers,
  // add new servers, etc.
  private void processAsActiveMaster(Set<ServerName> newServers) {
    Set<ServerName> oldServers = regionServers;
    ServerManager serverManager = server.getServerManager();
    // expire dead servers
    for (ServerName crashedServer : Sets.difference(oldServers, newServers)) {
      LOG.info("RegionServer ephemeral node deleted, processing expiration [{}]", crashedServer);
      serverManager.expireServer(crashedServer);
    }
    // check whether there are new servers, log them
    boolean newServerAdded = false;
    for (ServerName sn : newServers) {
      if (!oldServers.contains(sn)) {
        newServerAdded = true;
        LOG.info("RegionServer ephemeral node created, adding [" + sn + "]");
      }
    }
    if (newServerAdded && server.isInitialized()) {
      // Only call the check to move servers if a RegionServer was added to the cluster; in this
      // case it could be a server with a new version so it makes sense to run the check.
      server.checkIfShouldMoveSystemRegionAsync();
    }
  }

  private synchronized void refresh() {
    List<String> names;
    try {
      names = ZKUtil.listChildrenAndWatchForNewChildren(watcher, watcher.getZNodePaths().rsZNode);
    } catch (KeeperException e) {
      // here we need to abort as we failed to set watcher on the rs node which means that we can
      // not track the node deleted event any more.
      server.abort("Unexpected zk exception getting RS nodes", e);
      return;
    }
    Set<ServerName> newServers = CollectionUtils.isEmpty(names) ? Collections.emptySet() :
      names.stream().map(ServerName::parseServerName)
        .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    if (active) {
      processAsActiveMaster(newServers);
    }
    this.regionServers = newServers;
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.getZNodePaths().rsZNode) && !server.isAborted() &&
      !server.isStopped()) {
      executor.execute(this::refresh);
    }
  }
}

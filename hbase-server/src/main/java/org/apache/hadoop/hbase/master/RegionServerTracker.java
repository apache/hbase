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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
  private final Set<ServerName> regionServers = new HashSet<>();
  private final ServerManager serverManager;
  private final MasterServices server;
  // As we need to send request to zk when processing the nodeChildrenChanged event, we'd better
  // move the operation to a single threaded thread pool in order to not block the zk event
  // processing since all the zk listener across HMaster will be called in one thread sequentially.
  private final ExecutorService executor;

  public RegionServerTracker(ZKWatcher watcher, MasterServices server,
      ServerManager serverManager) {
    super(watcher);
    this.server = server;
    this.serverManager = serverManager;
    this.executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("RegionServerTracker-%d").build());
  }

  private Pair<ServerName, RegionServerInfo> getServerInfo(String name)
      throws KeeperException, IOException {
    ServerName serverName = ServerName.parseServerName(name);
    String nodePath = ZNodePaths.joinZNode(watcher.getZNodePaths().rsZNode, name);
    byte[] data;
    try {
      data = ZKUtil.getData(watcher, nodePath);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
    if (data == null) {
      // we should receive a children changed event later and then we will expire it, so we still
      // need to add it to the region server set.
      LOG.warn("Server node {} does not exist, already dead?", name);
      return Pair.newPair(serverName, null);
    }
    if (data.length == 0 || !ProtobufUtil.isPBMagicPrefix(data)) {
      // this should not happen actually, unless we have bugs or someone has messed zk up.
      LOG.warn("Invalid data for region server node {} on zookeeper, data length = {}", name,
        data.length);
      return Pair.newPair(serverName, null);
    }
    RegionServerInfo.Builder builder = RegionServerInfo.newBuilder();
    int magicLen = ProtobufUtil.lengthOfPBMagic();
    ProtobufUtil.mergeFrom(builder, data, magicLen, data.length - magicLen);
    return Pair.newPair(serverName, builder.build());
  }

  /**
   * Starts the tracking of online RegionServers. All RSes will be tracked after this method is
   * called.
   * <p/>
   * In this method, we will also construct the region server sets in {@link ServerManager}. If a
   * region server is dead between the crash of the previous master instance and the start of the
   * current master instance, we will schedule a SCP for it. This is done in
   * {@link ServerManager#findDeadServersAndProcess(Set, Set)}, we call it here under the lock
   * protection to prevent concurrency issues with server expiration operation.
   * @param deadServersFromPE the region servers which already have SCP associated.
   * @param liveServersFromWALDir the live region servers from wal directory.
   * @param splittingServersFromWALDir Servers whose WALs are being actively 'split'.
   */
  public void start(Set<ServerName> deadServersFromPE, Set<ServerName> liveServersFromWALDir,
      Set<ServerName> splittingServersFromWALDir)
      throws KeeperException, IOException {
    LOG.info("Starting RegionServerTracker; {} have existing ServerCrashProcedures, {} " +
        "possibly 'live' servers, and {} 'splitting'.", deadServersFromPE.size(),
        liveServersFromWALDir.size(), splittingServersFromWALDir.size());
    // deadServersFromPE is made from a list of outstanding ServerCrashProcedures.
    // splittingServersFromWALDir are being actively split -- the directory in the FS ends in
    // '-SPLITTING'. Each splitting server should have a corresponding SCP. Log if not.
    splittingServersFromWALDir.stream().filter(s -> !deadServersFromPE.contains(s)).
      forEach(s -> LOG.error("{} has no matching ServerCrashProcedure", s));
    //create ServerNode for all possible live servers from wal directory
    liveServersFromWALDir
        .forEach(sn -> server.getAssignmentManager().getRegionStates().getOrCreateServer(sn));
    watcher.registerListener(this);
    synchronized (this) {
      List<String> servers =
        ZKUtil.listChildrenAndWatchForNewChildren(watcher, watcher.getZNodePaths().rsZNode);
      if (null != servers) {
        for (String n : servers) {
          Pair<ServerName, RegionServerInfo> pair = getServerInfo(n);
          ServerName serverName = pair.getFirst();
          RegionServerInfo info = pair.getSecond();
          regionServers.add(serverName);
          ServerMetrics serverMetrics = info != null ?
            ServerMetricsBuilder.of(serverName, VersionInfoUtil.getVersionNumber(info.getVersionInfo()),
              info.getVersionInfo().getVersion()) :
            ServerMetricsBuilder.of(serverName);
          serverManager.checkAndRecordNewServer(serverName, serverMetrics);
        }
      }
      serverManager.findDeadServersAndProcess(deadServersFromPE, liveServersFromWALDir);
    }
  }

  public void stop() {
    executor.shutdownNow();
  }

  private synchronized void refresh() {
    List<String> names;
    try {
      names = ZKUtil.listChildrenAndWatchForNewChildren(watcher, watcher.getZNodePaths().rsZNode);
    } catch (KeeperException e) {
      // here we need to abort as we failed to set watcher on the rs node which means that we can
      // not track the node deleted evetnt any more.
      server.abort("Unexpected zk exception getting RS nodes", e);
      return;
    }
    Set<ServerName> servers = CollectionUtils.isEmpty(names) ? Collections.emptySet() :
      names.stream().map(ServerName::parseServerName).collect(Collectors.toSet());

    for (Iterator<ServerName> iter = regionServers.iterator(); iter.hasNext();) {
      ServerName sn = iter.next();
      if (!servers.contains(sn)) {
        LOG.info("RegionServer ephemeral node deleted, processing expiration [{}]", sn);
        serverManager.expireServer(sn);
        iter.remove();
      }
    }
    // here we do not need to parse the region server info as it is useless now, we only need the
    // server name.
    boolean newServerAdded = false;
    for (ServerName sn : servers) {
      if (regionServers.add(sn)) {
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

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.getZNodePaths().rsZNode) && !server.isAborted() &&
      !server.isStopped()) {
      executor.execute(this::refresh);
    }
  }
}

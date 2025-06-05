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
package org.apache.hadoop.hbase.testing;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@InterfaceAudience.Private
class TestingHBaseClusterImpl implements TestingHBaseCluster {

  private final HBaseTestingUtil util;

  private final StartTestingClusterOption option;

  private final String externalDfsUri;

  private final String externalZkConnectString;

  private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
    .setNameFormat(getClass().getSuperclass() + "-%d").setDaemon(true).build());

  private boolean miniClusterRunning = false;

  private boolean miniHBaseClusterRunning = false;

  TestingHBaseClusterImpl(TestingHBaseClusterOption option) {
    this.util = new HBaseTestingUtil(option.conf());
    this.option = option.convert();
    this.externalDfsUri = option.getExternalDfsUri();
    this.externalZkConnectString = option.getExternalZkConnectString();
  }

  @Override
  public Configuration getConf() {
    return util.getConfiguration();
  }

  private int getRegionServerIndex(ServerName serverName) {
    // we have a small number of region servers, this should be fine for now.
    List<RegionServerThread> servers = util.getMiniHBaseCluster().getRegionServerThreads();
    for (int i = 0; i < servers.size(); i++) {
      if (servers.get(i).getRegionServer().getServerName().equals(serverName)) {
        return i;
      }
    }
    return -1;
  }

  private int getMasterIndex(ServerName serverName) {
    List<MasterThread> masters = util.getMiniHBaseCluster().getMasterThreads();
    for (int i = 0; i < masters.size(); i++) {
      if (masters.get(i).getMaster().getServerName().equals(serverName)) {
        return i;
      }
    }
    return -1;
  }

  private void join(Thread thread, CompletableFuture<?> future) {
    executor.execute(() -> {
      try {
        thread.join();
        future.complete(null);
      } catch (InterruptedException e) {
        future.completeExceptionally(e);
      }
    });
  }

  @Override
  public CompletableFuture<Void> stopMaster(ServerName serverName) throws Exception {
    CompletableFuture<Void> future = new CompletableFuture<>();
    int index = getMasterIndex(serverName);
    if (index == -1) {
      future.completeExceptionally(new IllegalArgumentException("Unknown master " + serverName));
    }
    join(util.getMiniHBaseCluster().stopMaster(index), future);
    return future;
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName serverName) throws Exception {
    CompletableFuture<Void> future = new CompletableFuture<>();
    int index = getRegionServerIndex(serverName);
    if (index == -1) {
      future
        .completeExceptionally(new IllegalArgumentException("Unknown region server " + serverName));
    }
    join(util.getMiniHBaseCluster().stopRegionServer(index), future);
    return future;
  }

  @Override
  public void stopHBaseCluster() throws Exception {
    Preconditions.checkState(miniClusterRunning, "Cluster has already been stopped");
    Preconditions.checkState(miniHBaseClusterRunning, "HBase cluster has already been started");
    util.shutdownMiniHBaseCluster();
    miniHBaseClusterRunning = false;
  }

  @Override
  public void startHBaseCluster() throws Exception {
    Preconditions.checkState(miniClusterRunning, "Cluster has already been stopped");
    Preconditions.checkState(!miniHBaseClusterRunning, "HBase cluster has already been started");
    util.startMiniHBaseCluster(option);
    miniHBaseClusterRunning = true;
  }

  @Override
  public void start() throws Exception {
    Preconditions.checkState(!miniClusterRunning, "Cluster has already been started");
    if (externalZkConnectString == null) {
      util.startMiniZKCluster();
    } else {
      Configuration conf = util.getConfiguration();
      conf.set(HConstants.ZOOKEEPER_QUORUM, externalZkConnectString);
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/" + UUID.randomUUID().toString());
    }
    if (externalDfsUri == null) {
      util.startMiniDFSCluster(option.getNumDataNodes(), option.getDataNodeHosts());
    } else {
      Configuration conf = util.getConfiguration();
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, externalDfsUri);
    }
    util.startMiniHBaseCluster(option);
    miniClusterRunning = true;
    miniHBaseClusterRunning = true;
  }

  @Override
  public void stop() throws Exception {
    Preconditions.checkState(miniClusterRunning, "Cluster has already been stopped");
    util.shutdownMiniCluster();
    miniClusterRunning = false;
    miniHBaseClusterRunning = false;
  }

  @Override
  public boolean isHBaseClusterRunning() {
    return miniHBaseClusterRunning;
  }

  @Override
  public boolean isClusterRunning() {
    return miniClusterRunning;
  }

  @Override
  public void startMaster() throws Exception {
    util.getMiniHBaseCluster().startMaster();
  }

  @Override
  public void startMaster(String hostname, int port) throws Exception {
    util.getMiniHBaseCluster().startMaster(hostname, port);
  }

  @Override
  public void startRegionServer() throws Exception {
    util.getMiniHBaseCluster().startRegionServer();
  }

  @Override
  public void startRegionServer(String hostname, int port) throws Exception {
    util.getMiniHBaseCluster().startRegionServer(hostname, port);
  }

  @Override
  public Optional<ServerName> getActiveMasterAddress() {
    return Optional.ofNullable(util.getMiniHBaseCluster().getMaster()).map(HMaster::getServerName);
  }

  @Override
  public List<ServerName> getBackupMasterAddresses() {
    return util.getMiniHBaseCluster().getMasterThreads().stream().map(MasterThread::getMaster)
      .filter(m -> !m.isActiveMaster()).map(HMaster::getServerName).collect(Collectors.toList());
  }

  @Override
  public List<ServerName> getRegionServerAddresses() {
    return util.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getServerName()).collect(Collectors.toList());
  }

  @Override
  public Optional<Integer> getActiveMasterInfoPort() {
    return Optional.ofNullable(util.getMiniHBaseCluster().getMaster())
      .map(m -> m.getConfiguration().getInt(HConstants.MASTER_INFO_PORT, 0))
      .filter(port -> port > 0);
  }

  @Override
  public Optional<Integer> getActiveNameNodeInfoPort() {
    return Stream.of(util.getDFSCluster().getNameNodeInfos()).map(i -> i.nameNode)
      .filter(NameNode::isActiveState).map(nn -> nn.getHttpAddress().getPort())
      .filter(port -> port > 0).findFirst();
  }

  @Override
  public Optional<Integer> getActiveZooKeeperClientPort() {
    return Optional.ofNullable(util.getZkCluster()).map(MiniZooKeeperCluster::getClientPort)
      .filter(port -> port > 0);
  }

  @Override
  public List<String> getMasterAddresses() {
    return util.getMiniHBaseCluster().getMasterThreads().stream()
      .map(mt -> mt.getMaster().getServerName()).map(sn -> sn.getHostname() + ':' + sn.getPort())
      .collect(Collectors.toList());
  }

  @Override
  public Optional<Region> getRegion(RegionInfo regionInfo) {
    for (RegionServerThread t : util.getMiniHBaseCluster().getRegionServerThreads()) {
      for (HRegion region : t.getRegionServer().getRegions()) {
        if (region.getRegionInfo().equals(regionInfo)) {
          return Optional.of(region);
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<OnlineRegions> getOnlineRegionsInterface(ServerName serverName) {
    return Optional.ofNullable(util.getMiniHBaseCluster().getRegionServer(serverName));
  }
}

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
package org.apache.hadoop.hbase;

import com.google.protobuf.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Mock region server services with WALProvider, it can be used for testing wal related tests,
 * like split or merge regions.
 */
public class MockRegionServerServicesWithWALs implements RegionServerServices {
  WALProvider provider;
  RegionServerServices rss;

  public MockRegionServerServicesWithWALs(RegionServerServices rss, WALProvider provider) {
    this.rss = rss;
    this.provider = provider;
  }

  @Override
  public boolean isStopping() {
    return rss.isStopping();
  }

  @Override
  public WAL getWAL(HRegionInfo hri) throws IOException {
    return provider.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return rss.getCompactionRequester();
  }

  @Override
  public FlushRequester getFlushRequester() {
    return rss.getFlushRequester();
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return rss.getRegionServerAccounting();
  }

  @Override
  public TableLockManager getTableLockManager() {
    return rss.getTableLockManager();
  }

  @Override
  public RegionServerQuotaManager getRegionServerQuotaManager() {
    return rss.getRegionServerQuotaManager();
  }

  @Override
  public void postOpenDeployTasks(PostOpenDeployContext context)
    throws KeeperException, IOException {
    rss.postOpenDeployTasks(context);
  }

  @Override
  public void postOpenDeployTasks(Region r) throws KeeperException, IOException {
    rss.postOpenDeployTasks(r);
  }

  @Override
  public boolean reportRegionStateTransition(RegionStateTransitionContext context) {
    return rss.reportRegionStateTransition(context);
  }

  @Override
  public boolean reportRegionStateTransition(
    RegionServerStatusProtos.RegionStateTransition.TransitionCode code, long openSeqNum,
    HRegionInfo... hris) {
    return rss.reportRegionStateTransition(code, openSeqNum, hris);
  }

  @Override
  public boolean reportRegionStateTransition(
    RegionServerStatusProtos.RegionStateTransition.TransitionCode code, HRegionInfo... hris) {
    return rss.reportRegionStateTransition(code, hris);
  }

  @Override
  public RpcServerInterface getRpcServer() {
    return rss.getRpcServer();
  }

  @Override
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return rss.getRegionsInTransitionInRS();
  }

  @Override
  public FileSystem getFileSystem() {
    return rss.getFileSystem();
  }

  @Override
  public Leases getLeases() {
    return rss.getLeases();
  }

  @Override
  public ExecutorService getExecutorService() {
    return rss.getExecutorService();
  }

  @Override
  public Map<String, Region> getRecoveringRegions() {
    return rss.getRecoveringRegions();
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return rss.getNonceManager();
  }

  @Override
  public boolean registerService(Service service) {
    return rss.registerService(service);
  }

  @Override
  public HeapMemoryManager getHeapMemoryManager() {
    return rss.getHeapMemoryManager();
  }

  @Override
  public double getCompactionPressure() {
    return rss.getCompactionPressure();
  }

  @Override
  public Set<TableName> getOnlineTables() {
    return rss.getOnlineTables();
  }

  @Override
  public ThroughputController getFlushThroughputController() {
    return rss.getFlushThroughputController();
  }

  @Override
  public double getFlushPressure() {
    return rss.getFlushPressure();
  }

  @Override
  public MetricsRegionServer getMetrics() {
    return rss.getMetrics();
  }

  @Override
  public void unassign(byte[] regionName) throws IOException {
    rss.unassign(regionName);
  }

  @Override
  public void addToOnlineRegions(Region r) {
    rss.addToOnlineRegions(r);
  }

  @Override
  public boolean removeFromOnlineRegions(Region r, ServerName destination) {
    return rss.removeFromOnlineRegions(r, destination);
  }

  @Override
  public Region getFromOnlineRegions(String encodedRegionName) {
    return rss.getFromOnlineRegions(encodedRegionName);
  }

  @Override
  public List<Region> getOnlineRegions(TableName tableName) throws IOException {
    return rss.getOnlineRegions(tableName);
  }

  @Override
  public List<Region> getOnlineRegions() {
    return rss.getOnlineRegions();
  }

  @Override
  public Configuration getConfiguration() {
    return rss.getConfiguration();
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return rss.getZooKeeper();
  }

  @Override
  public ClusterConnection getConnection() {
    return rss.getConnection();
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return rss.getMetaTableLocator();
  }

  @Override
  public ServerName getServerName() {
    return rss.getServerName();
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return rss.getCoordinatedStateManager();
  }

  @Override
  public ChoreService getChoreService() {
    return rss.getChoreService();
  }

  @Override
  public void abort(String why, Throwable e) {
    rss.abort(why, e);
  }

  @Override
  public boolean isAborted() {
    return rss.isAborted();
  }

  @Override
  public void stop(String why) {
    rss.stop(why);
  }

  @Override
  public boolean isStopped() {
    return rss.isStopped();
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName,
    List<HBaseProtos.ServerName> favoredNodes) {
    rss.updateRegionFavoredNodesMapping(encodedRegionName, favoredNodes);
  }

  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return rss.getFavoredNodesForRegion(encodedRegionName);
  }
}

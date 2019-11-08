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
package org.apache.hadoop.hbase;

import com.google.protobuf.Service;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.locking.EntityLock;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.LeaseManager;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.SecureBulkLoadManager;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Basic mock region server services.  Should only be instantiated by HBaseTestingUtility.b
 */
public class MockRegionServerServices implements RegionServerServices {
  protected static final Logger LOG = LoggerFactory.getLogger(MockRegionServerServices.class);
  private final Map<String, Region> regions = new HashMap<>();
  private final ConcurrentSkipListMap<byte[], Boolean> rit =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
  private HFileSystem hfs = null;
  private final Configuration conf;
  private ZKWatcher zkw = null;
  private ServerName serverName = null;
  private RpcServerInterface rpcServer = null;
  private volatile boolean abortRequested;
  private volatile boolean stopping = false;
  private final AtomicBoolean running = new AtomicBoolean(true);

  MockRegionServerServices(ZKWatcher zkw) {
    this(zkw, null);
  }

  MockRegionServerServices(ZKWatcher zkw, ServerName serverName) {
    this.zkw = zkw;
    this.serverName = serverName;
    this.conf = (zkw == null ? new Configuration() : zkw.getConfiguration());
  }

  MockRegionServerServices(){
    this(null, null);
  }

  public MockRegionServerServices(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean removeRegion(HRegion r, ServerName destination) {
    return this.regions.remove(r.getRegionInfo().getEncodedName()) != null;
  }

  @Override
  public Region getRegion(String encodedRegionName) {
    return this.regions.get(encodedRegionName);
  }

  @Override
  public List<Region> getRegions(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public List<Region> getRegions() {
    return null;
  }

  @Override
  public void addRegion(HRegion r) {
    this.regions.put(r.getRegionInfo().getEncodedName(), r);
  }

  @Override
  public void postOpenDeployTasks(PostOpenDeployContext context) throws IOException {
    addRegion(context.getRegion());
  }

  @Override
  public boolean isStopping() {
    return this.stopping;
  }

  @Override
  public RpcServerInterface getRpcServer() {
    return rpcServer;
  }

  public void setRpcServer(RpcServerInterface rpc) {
    this.rpcServer = rpc;
  }

  @Override
  public ConcurrentSkipListMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return rit;
  }

  @Override
  public FlushRequester getFlushRequester() {
    return null;
  }

  @Override
  public CompactionRequester getCompactionRequestor() {
    return null;
  }

  @Override
  public ClusterConnection getConnection() {
    return null;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zkw;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return null;
  }

  @Override
  public RegionServerRpcQuotaManager getRegionServerRpcQuotaManager() {
    return null;
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void abort(String why, Throwable e) {
    this.abortRequested = true;
    stop(why);
  }

  @Override
  public void stop(String why) {
    this.stopping = true;
    if (running.compareAndSet(true, false)) {
      LOG.info("Shutting down due to request '" + why + "'");
    }
  }

  @Override
  public boolean isStopped() {
    return !(running.get());
  }

  @Override
  public boolean isAborted() {
    return this.abortRequested;
  }

  @Override
  public HFileSystem getFileSystem() {
    return this.hfs;
  }

  public void setFileSystem(FileSystem hfs) {
    this.hfs = (HFileSystem)hfs;
  }

  @Override
  public LeaseManager getLeaseManager() {
    return null;
  }

  @Override
  public List<WAL> getWALs() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public WAL getWAL(RegionInfo regionInfo) throws IOException {
    return null;
  }

  @Override
  public ExecutorService getExecutorService() {
    return null;
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName,
      List<HBaseProtos.ServerName> favoredNodes) {
  }

  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return null;
  }

  @Override
  public ServerNonceManager getNonceManager() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean reportRegionStateTransition(RegionStateTransitionContext context) {
    return false;
  }

  @Override
  public boolean registerService(Service service) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public HeapMemoryManager getHeapMemoryManager() {
    return null;
  }

  @Override
  public double getCompactionPressure() {
    return 0;
  }

  @Override
  public ClusterConnection getClusterConnection() {
    return null;
  }

  @Override
  public ThroughputController getFlushThroughputController() {
    return null;
  }

  @Override
  public double getFlushPressure() {
    return 0;
  }

  @Override
  public MetricsRegionServer getMetrics() {
    return null;
  }

  @Override
  public EntityLock regionLock(List<RegionInfo> regionInfos, String description, Abortable abort)
      throws IOException {
    return null;
  }

  @Override
  public SecureBulkLoadManager getSecureBulkLoadManager() {
    return null;
  }

  @Override
  public void unassign(byte[] regionName) throws IOException {
  }

  @Override
  public RegionServerSpaceQuotaManager getRegionServerSpaceQuotaManager() {
    return null;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    return null;
  }

  @Override
  public boolean isClusterUp() {
    return true;
  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return null;
  }

  @Override
  public Optional<BlockCache> getBlockCache() {
    return Optional.empty();
  }

  @Override
  public Optional<MobFileCache> getMobFileCache() {
    return Optional.empty();
  }

  @Override
  public AccessChecker getAccessChecker() {
    return null;
  }

  @Override
  public ZKPermissionWatcher getZKPermissionWatcher() {
    return null;
  }

  @Override
  public boolean reportRegionSizesForQuotas(RegionSizeStore sizeStore) {
    return true;
  }

  @Override
  public boolean reportFileArchivalForQuotas(
      TableName tableName, Collection<Entry<String,Long>> archivedFiles) {
    return true;
  }
}

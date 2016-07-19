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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.NullTableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.SecureBulkLoadManager;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.Service;

/**
 * Basic mock region server services.  Should only be instantiated by HBaseTestingUtility.b
 */
public class MockRegionServerServices implements RegionServerServices {
  protected static final Log LOG = LogFactory.getLog(MockRegionServerServices.class);
  private final Map<String, Region> regions = new HashMap<String, Region>();
  private final ConcurrentSkipListMap<byte[], Boolean> rit =
    new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);
  private HFileSystem hfs = null;
  private final Configuration conf;
  private ZooKeeperWatcher zkw = null;
  private ServerName serverName = null;
  private RpcServerInterface rpcServer = null;
  private volatile boolean abortRequested;
  private volatile boolean stopping = false;
  private final AtomicBoolean running = new AtomicBoolean(true);

  MockRegionServerServices(ZooKeeperWatcher zkw) {
    this(zkw, null);
  }

  MockRegionServerServices(ZooKeeperWatcher zkw, ServerName serverName) {
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
  public boolean removeFromOnlineRegions(Region r, ServerName destination) {
    return this.regions.remove(r.getRegionInfo().getEncodedName()) != null;
  }

  @Override
  public Region getFromOnlineRegions(String encodedRegionName) {
    return this.regions.get(encodedRegionName);
  }

  @Override
  public List<Region> getOnlineRegions(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public Set<TableName> getOnlineTables() {
    return null;
  }

  @Override
  public List<Region> getOnlineRegions() {
    return null;
  }

  @Override
  public void addToOnlineRegions(Region r) {
    this.regions.put(r.getRegionInfo().getEncodedName(), r);
  }

  @Override
  public void postOpenDeployTasks(Region r) throws KeeperException, IOException {
    addToOnlineRegions(r);
  }

  @Override
  public void postOpenDeployTasks(PostOpenDeployContext context) throws KeeperException,
      IOException {
    addToOnlineRegions(context.getRegion());
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
  public CompactionRequestor getCompactionRequester() {
    return null;
  }

  @Override
  public ClusterConnection getConnection() {
    return null;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return null;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
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
  public TableLockManager getTableLockManager() {
    return new NullTableLockManager();
  }

  @Override
  public RegionServerQuotaManager getRegionServerQuotaManager() {
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
  public Leases getLeases() {
    return null;
  }

  @Override
  public WAL getWAL(HRegionInfo regionInfo) throws IOException {
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
      List<org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
  }

  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return null;
  }

  @Override
  public Map<String, Region> getRecoveringRegions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServerNonceManager getNonceManager() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean reportRegionStateTransition(TransitionCode code, long openSeqNum,
      HRegionInfo... hris) {
    return false;
  }

  @Override
  public boolean reportRegionStateTransition(TransitionCode code,
      HRegionInfo... hris) {
    return false;
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
  public SecureBulkLoadManager getSecureBulkLoadManager() {
    return null;
  }
}

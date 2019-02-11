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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.locking.EntityLock;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ReplicationSourceService;
import org.apache.hadoop.hbase.regionserver.SecureBulkLoadManager;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;

/**
 * A mock RegionServer implementation.
 * Use this when you can't bend Mockito to your liking (e.g. return null result
 * when 'scanning' until master timesout and then return a coherent meta row
 * result thereafter.  Have some facility for faking gets and scans.  See
 * setGetResult(byte[], byte[], Result) for how to fill the backing data
 * store that the get pulls from.
 */
class MockRegionServer implements AdminProtos.AdminService.BlockingInterface,
    ClientProtos.ClientService.BlockingInterface, RegionServerServices {
  private final ServerName sn;
  private final ZKWatcher zkw;
  private final Configuration conf;
  private final Random random = new Random();

  /**
   * Map of regions to map of rows and {@link Result}.  Used as data source when
   * {@link #get(RpcController, GetRequest)} is called. Because we have a byte
   * key, need to use TreeMap and provide a Comparator.  Use
   * {@link #setGetResult(byte[], byte[], Result)} filling this map.
   */
  private final Map<byte [], Map<byte [], Result>> gets = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * Map of regions to results to return when scanning.
   */
  private final Map<byte [], Result []> nexts = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * Data structure that holds regionname and index used scanning.
   */
  class RegionNameAndIndex {
    private final byte[] regionName;
    private int index = 0;

    RegionNameAndIndex(final byte[] regionName) {
      this.regionName = regionName;
    }

    byte[] getRegionName() {
      return this.regionName;
    }

    int getThenIncrement() {
      int currentIndex = this.index;
      this.index++;
      return currentIndex;
    }
  }

  /**
   * Outstanding scanners and their offset into <code>nexts</code>
   */
  private final Map<Long, RegionNameAndIndex> scannersAndOffsets = new HashMap<>();

  /**
   * @param sn Name of this mock regionserver
   * @throws IOException
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   */
  MockRegionServer(final Configuration conf, final ServerName sn)
  throws ZooKeeperConnectionException, IOException {
    this.sn = sn;
    this.conf = conf;
    this.zkw = new ZKWatcher(conf, sn.toString(), this, true);
  }

  /**
   * Use this method filling the backing data source used by {@link #get(RpcController, GetRequest)}
   * @param regionName
   * @param row
   * @param r
   */
  void setGetResult(final byte [] regionName, final byte [] row, final Result r) {
    Map<byte [], Result> value = this.gets.get(regionName);
    if (value == null) {
      // If no value already, create one.  Needs to be treemap because we are
      // using byte array as key.   Not thread safe.
      value = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.gets.put(regionName, value);
    }
    value.put(row, r);
  }

  /**
   * Use this method to set what a scanner will reply as we next through
   * @param regionName
   * @param rs
   */
  void setNextResults(final byte [] regionName, final Result [] rs) {
    this.nexts.put(regionName, rs);
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public void abort(String why, Throwable e) {
    throw new RuntimeException(this.sn + ": " + why, e);
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  public long openScanner(byte[] regionName, Scan scan) throws IOException {
    long scannerId = this.random.nextLong();
    this.scannersAndOffsets.put(scannerId, new RegionNameAndIndex(regionName));
    return scannerId;
  }

  public Result next(long scannerId) throws IOException {
    RegionNameAndIndex rnai = this.scannersAndOffsets.get(scannerId);
    int index = rnai.getThenIncrement();
    Result [] results = this.nexts.get(rnai.getRegionName());
    if (results == null) return null;
    return index < results.length? results[index]: null;
  }

  public Result [] next(long scannerId, int numberOfRows) throws IOException {
    // Just return one result whatever they ask for.
    Result r = next(scannerId);
    return r == null? null: new Result [] {r};
  }

  public void close(final long scannerId) throws IOException {
    this.scannersAndOffsets.remove(scannerId);
  }

  @Override
  public void stop(String why) {
    this.zkw.close();
  }

  @Override
  public void addRegion(HRegion r) {
  }

  @Override
  public boolean removeRegion(HRegion r, ServerName destination) {
    return false;
  }

  @Override
  public HRegion getRegion(String encodedRegionName) {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return this.zkw;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public Connection getConnection() {
    return null;
  }

  @Override
  public ServerName getServerName() {
    return this.sn;
  }

  @Override
  public boolean isStopping() {
    return false;
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
  public RegionServerAccounting getRegionServerAccounting() {
    return null;
  }

  @Override
  public RegionServerRpcQuotaManager getRegionServerRpcQuotaManager() {
    return null;
  }

  @Override
  public void postOpenDeployTasks(PostOpenDeployContext context) throws IOException {
  }

  @Override
  public RpcServerInterface getRpcServer() {
    return null;
  }

  @Override
  public ConcurrentSkipListMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return null;
  }

  @Override
  public FileSystem getFileSystem() {
    return null;
  }

  @Override
  public GetResponse get(RpcController controller, GetRequest request)
  throws ServiceException {
    byte[] regionName = request.getRegion().getValue().toByteArray();
    Map<byte [], Result> m = this.gets.get(regionName);
    GetResponse.Builder builder = GetResponse.newBuilder();
    if (m != null) {
      byte[] row = request.getGet().getRow().toByteArray();
      builder.setResult(ProtobufUtil.toResult(m.get(row)));
    }
    return builder.build();
  }

  @Override
  public MutateResponse mutate(RpcController controller, MutateRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public ScanResponse scan(RpcController controller, ScanRequest request)
      throws ServiceException {
    ScanResponse.Builder builder = ScanResponse.newBuilder();
    try {
      if (request.hasScan()) {
        byte[] regionName = request.getRegion().getValue().toByteArray();
        builder.setScannerId(openScanner(regionName, null));
        builder.setMoreResults(true);
      }
      else {
        long scannerId = request.getScannerId();
        Result result = next(scannerId);
        if (result != null) {
          builder.addCellsPerResult(result.size());
          List<CellScannable> results = new ArrayList<>(1);
          results.add(result);
          ((HBaseRpcController) controller).setCellScanner(CellUtil
              .createCellScanner(results));
          builder.setMoreResults(true);
        }
        else {
          builder.setMoreResults(false);
          close(scannerId);
        }
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    return builder.build();
  }

  @Override
  public BulkLoadHFileResponse bulkLoadHFile(RpcController controller,
      BulkLoadHFileRequest request) throws ServiceException {
    return null;
  }

  @Override
  public ClientProtos.CoprocessorServiceResponse execService(RpcController controller,
      ClientProtos.CoprocessorServiceRequest request) throws ServiceException {
    return null;
  }

  @Override
  public ClientProtos.MultiResponse multi(
      RpcController controller, MultiRequest request) throws ServiceException {
    return null;
  }

  @Override
  public GetRegionInfoResponse getRegionInfo(RpcController controller,
      GetRegionInfoRequest request) throws ServiceException {
    GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
    builder.setRegionInfo(ProtobufUtil.toRegionInfo(RegionInfoBuilder.FIRST_META_REGIONINFO));
    return builder.build();
  }

  @Override
  public GetRegionLoadResponse getRegionLoad(RpcController controller,
      GetRegionLoadRequest request) throws ServiceException {
    GetRegionLoadResponse.Builder builder = GetRegionLoadResponse.newBuilder();
    return builder.build();
  }

  @Override
  public ClearCompactionQueuesResponse clearCompactionQueues(RpcController controller,
    ClearCompactionQueuesRequest request) throws ServiceException {
    return null;
  }

  @Override
  public GetStoreFileResponse getStoreFile(RpcController controller,
      GetStoreFileRequest request) throws ServiceException {
    return null;
  }

  @Override
  public GetOnlineRegionResponse getOnlineRegion(RpcController controller,
      GetOnlineRegionRequest request) throws ServiceException {
    return null;
  }

  @Override
  public List<Region> getRegions() {
    return null;
  }

  @Override
  public OpenRegionResponse openRegion(RpcController controller,
      OpenRegionRequest request) throws ServiceException {
    return null;
  }

  @Override
  public WarmupRegionResponse warmupRegion(RpcController controller,
      WarmupRegionRequest request) throws ServiceException {
    return null;
  }
  @Override
  public CloseRegionResponse closeRegion(RpcController controller,
      CloseRegionRequest request) throws ServiceException {
    return null;
  }

  @Override
  public FlushRegionResponse flushRegion(RpcController controller,
      FlushRegionRequest request) throws ServiceException {
    return null;
  }

  @Override
  public CompactionSwitchResponse compactionSwitch(RpcController controller,
      CompactionSwitchRequest request) throws ServiceException {
    return null;
  }

  @Override
  public CompactRegionResponse compactRegion(RpcController controller,
      CompactRegionRequest request) throws ServiceException {
    return null;
  }

  @Override
  public ReplicateWALEntryResponse replicateWALEntry(RpcController controller,
      ReplicateWALEntryRequest request) throws ServiceException {
    return null;
  }

  @Override
  public RollWALWriterResponse rollWALWriter(RpcController controller,
      RollWALWriterRequest request) throws ServiceException {
    return null;
  }

  @Override
  public GetServerInfoResponse getServerInfo(RpcController controller,
      GetServerInfoRequest request) throws ServiceException {
    return null;
  }

  @Override
  public StopServerResponse stopServer(RpcController controller,
      StopServerRequest request) throws ServiceException {
    return null;
  }

  @Override
  public List<Region> getRegions(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public Leases getLeases() {
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
      List<org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
  }

  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return null;
  }

  @Override
  public ReplicateWALEntryResponse
      replay(RpcController controller, ReplicateWALEntryRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
      UpdateFavoredNodesRequest request) throws ServiceException {
    return null;
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return null;
  }

  @Override
  public boolean reportRegionStateTransition(RegionStateTransitionContext context) {
    return false;
  }

  @Override
  public boolean registerService(com.google.protobuf.Service service) {
    return false;
  }

  @Override
  public CoprocessorServiceResponse execRegionServerService(RpcController controller,
      CoprocessorServiceRequest request) throws ServiceException {
    return null;
  }

  @Override
  public UpdateConfigurationResponse updateConfiguration(
      RpcController controller, UpdateConfigurationRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller,
                                                             ClearRegionBlockCacheRequest request)
    throws ServiceException {
    return null;
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
  public PrepareBulkLoadResponse prepareBulkLoad(RpcController controller,
      PrepareBulkLoadRequest request) throws ServiceException {
    return null;
  }

  @Override
  public CleanupBulkLoadResponse cleanupBulkLoad(RpcController controller,
      CleanupBulkLoadRequest request) throws ServiceException {
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
  public ExecuteProceduresResponse executeProcedures(RpcController controller,
      ExecuteProceduresRequest request) throws ServiceException {
    return null;
  }

  @Override
  public GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(
      RpcController controller, GetSpaceQuotaSnapshotsRequest request)
      throws ServiceException {
    return null;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    return null;
  }

  @Override
  public boolean reportRegionSizesForQuotas(RegionSizeStore sizeStore) {
    return true;
  }

  @Override
  public boolean reportFileArchivalForQuotas(
      TableName tableName, Collection<Entry<String, Long>> archivedFiles) {
    return false;
  }

  public boolean isClusterUp() {
    return true;
  }

  @Override
  public ReplicationSourceService getReplicationSourceService() {
    return null;
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

  public AsyncClusterConnection getAsyncClusterConnection() {
    return null;
  }
}

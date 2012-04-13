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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ClientProtocol;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowResponse;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * A mock RegionServer implementation.
 * Use this when you can't bend Mockito to your liking (e.g. return null result
 * when 'scanning' until master timesout and then return a coherent meta row
 * result thereafter.  Have some facility for faking gets and scans.  See
 * {@link #setGetResult(byte[], byte[], Result)} for how to fill the backing data
 * store that the get pulls from.
 */
class MockRegionServer implements HRegionInterface, ClientProtocol, RegionServerServices {
  private final ServerName sn;
  private final ZooKeeperWatcher zkw;
  private final Configuration conf;
  private final Random random = new Random();

  /**
   * Map of regions to map of rows and {@link Results}.  Used as data source when
   * {@link MockRegionServer#get(byte[], Get)} is called. Because we have a byte
   * key, need to use TreeMap and provide a Comparator.  Use
   * {@link #setGetResult(byte[], byte[], Result)} filling this map.
   */
  private final Map<byte [], Map<byte [], Result>> gets =
    new TreeMap<byte [], Map<byte [], Result>>(Bytes.BYTES_COMPARATOR);

  /**
   * Map of regions to results to return when scanning.
   */
  private final Map<byte [], Result []> nexts =
    new TreeMap<byte [], Result []>(Bytes.BYTES_COMPARATOR);

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
  private final Map<Long, RegionNameAndIndex> scannersAndOffsets =
    new HashMap<Long, RegionNameAndIndex>();

  /**
   * @param sn Name of this mock regionserver
   * @throws IOException 
   * @throws ZooKeeperConnectionException 
   */
  MockRegionServer(final Configuration conf, final ServerName sn)
  throws ZooKeeperConnectionException, IOException {
    this.sn = sn;
    this.conf = conf;
    this.zkw = new ZooKeeperWatcher(conf, sn.toString(), this, true);
  }

  /**
   * Use this method filling the backing data source used by {@link #get(byte[], Get)}
   * @param regionName
   * @param row
   * @param r
   */
  void setGetResult(final byte [] regionName, final byte [] row, final Result r) {
    Map<byte [], Result> value = this.gets.get(regionName);
    if (value == null) {
      // If no value already, create one.  Needs to be treemap because we are
      // using byte array as key.   Not thread safe.
      value = new TreeMap<byte [], Result>(Bytes.BYTES_COMPARATOR);
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
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isStopped() {
    // TODO Auto-generated method stub
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

  @Override
  public HRegionInfo getRegionInfo(byte[] regionName) {
    // Just return this.  Calls to getRegionInfo are usually to test connection
    // to regionserver does reasonable things so should be safe to return
    // anything.
    return HRegionInfo.ROOT_REGIONINFO;
  }

  @Override
  public void flushRegion(byte[] regionName) throws IllegalArgumentException,
      IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void flushRegion(byte[] regionName, long ifOlderThanTS)
      throws IllegalArgumentException, IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public long getLastFlushTime(byte[] regionName) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName, byte[] columnFamily)
      throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName,
      byte[][] columnFamilies) throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName)
      throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Result getClosestRowBefore(byte[] regionName, byte[] row,
      byte[] family) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Result get(byte[] regionName, Get get) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean exists(byte[] regionName, Get get) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void put(byte[] regionName, Put put) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public int put(byte[] regionName, List<Put> puts) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void delete(byte[] regionName, Delete delete) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public int delete(byte[] regionName, List<Delete> deletes)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, byte[] value, Put put) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, byte[] value, Delete delete) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long incrementColumnValue(byte[] regionName, byte[] row,
      byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Result append(byte[] regionName, Append append) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Result increment(byte[] regionName, Increment increment)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long openScanner(byte[] regionName, Scan scan) throws IOException {
    long scannerId = this.random.nextLong();
    this.scannersAndOffsets.put(scannerId, new RegionNameAndIndex(regionName));
    return scannerId;
  }

  @Override
  public Result next(long scannerId) throws IOException {
    RegionNameAndIndex rnai = this.scannersAndOffsets.get(scannerId);
    int index = rnai.getThenIncrement();
    Result [] results = this.nexts.get(rnai.getRegionName());
    if (results == null) return null;
    return index < results.length? results[index]: null;
  }

  @Override
  public Result [] next(long scannerId, int numberOfRows) throws IOException {
    // Just return one result whatever they ask for.
    Result r = next(scannerId);
    return r == null? null: new Result [] {r};
  }

  @Override
  public void close(final long scannerId) throws IOException {
    this.scannersAndOffsets.remove(scannerId);
  }

  @Override
  public long lockRow(byte[] regionName, byte[] row) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void unlockRow(byte[] regionName, long lockId) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public List<HRegionInfo> getOnlineRegions() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HRegion> getOnlineRegions(byte[] tableName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HServerInfo getHServerInfo() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <R> MultiResponse multi(MultiAction<R> multi) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths,
      byte[] regionName) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public RegionOpeningState openRegion(HRegionInfo region) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RegionOpeningState openRegion(HRegionInfo region,
      int versionOfOfflineNode) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void openRegions(List<HRegionInfo> regions) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean closeRegion(HRegionInfo region) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean closeRegion(HRegionInfo region, int versionOfClosingNode)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean closeRegion(HRegionInfo region, boolean zk)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean closeRegion(byte[] encodedRegionName, boolean zk)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void flushRegion(HRegionInfo regionInfo)
      throws NotServingRegionException, IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void splitRegion(HRegionInfo regionInfo)
      throws NotServingRegionException, IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void splitRegion(HRegionInfo regionInfo, byte[] splitPoint)
      throws NotServingRegionException, IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void compactRegion(HRegionInfo regionInfo, boolean major)
      throws NotServingRegionException, IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public void replicateLogEntries(Entry[] entries) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public ExecResult execCoprocessor(byte[] regionName, Exec call)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, CompareOp compareOp,
      WritableByteArrayComparable comparator, Put put) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, CompareOp compareOp,
      WritableByteArrayComparable comparator, Delete delete)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries()
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[][] rollHLogWriter() throws IOException,
      FailedLogCloseException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void stop(String why) {
    this.zkw.close();
  }

  @Override
  public void addToOnlineRegions(HRegion r) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean removeFromOnlineRegions(String encodedRegionName) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public HRegion getFromOnlineRegions(String encodedRegionName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return this.zkw;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    // TODO Auto-generated method stub
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
  public HLog getWAL() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FlushRequester getFlushRequester() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void postOpenDeployTasks(HRegion r, CatalogTracker ct, boolean daughter)
      throws KeeperException, IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public RpcServer getRpcServer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<byte[], Boolean> getRegionsInTransitionInRS() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FileSystem getFileSystem() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void mutateRow(byte[] regionName, RowMutations rm) throws IOException {
    // TODO Auto-generated method stub
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
    // TODO Auto-generated method stub
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
          builder.addResult(ProtobufUtil.toResult(result));
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
  public LockRowResponse lockRow(RpcController controller,
      LockRowRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public UnlockRowResponse unlockRow(RpcController controller,
      UnlockRowRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BulkLoadHFileResponse bulkLoadHFile(RpcController controller,
      BulkLoadHFileRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ExecCoprocessorResponse execCoprocessor(RpcController controller,
      ExecCoprocessorRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse multi(
      RpcController controller, MultiRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }
}
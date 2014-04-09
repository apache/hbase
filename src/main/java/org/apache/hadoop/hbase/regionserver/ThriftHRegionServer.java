/*
 * Copyright The Apache Software Foundation
 *
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TMultiResponse;
import org.apache.hadoop.hbase.client.TRowMutations;
import org.apache.hadoop.hbase.coprocessor.endpoints.EndpointServer;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.master.AssignmentPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This is just a wrapper around {@link HRegionServer}
 *
 */
public class ThriftHRegionServer implements ThriftHRegionInterface {
  public static Log LOG = LogFactory.getLog(ThriftHRegionServer.class);

  private HRegionServer server;
  private EndpointServer endpointServer;

  public ThriftHRegionServer(HRegionServer server) {
    this.server = server;
    this.endpointServer = new EndpointServer(this.server);
  }

  @Override
  public HRegionInfo getRegionInfo(byte[] regionName)
      throws ThriftHBaseException {
    try {
      HRegionInfo r = server.getRegionInfo(regionName);
      LOG.debug("Printing the result of getClosestRowOrBefore : " + r);
      return r;
    } catch (NotServingRegionException e) {
      e.printStackTrace();
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public Result getClosestRowBefore(byte[] regionName, byte[] row, byte[] family)
      throws ThriftHBaseException {
    try {
      Result r =  server.getClosestRowBefore(regionName, row, family);
      if (r == null) {
        return Result.SENTINEL_RESULT;
      } else {
        return addThriftRegionInfoQualifierIfNeeded(r);
      }
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public ListenableFuture<Result> getClosestRowBeforeAsync(final byte[] regionName,
      final byte[] row, final byte[] family) {
    try {
      Result result = getClosestRowBefore(regionName, row, family);
      return Futures.immediateFuture(result);
    } catch (ThriftHBaseException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * If there is a 'regioninfo' value in the result, it would be in Writable
   * serialized form. Add a thrift serialized HRegionInfo object for the
   * non-Java objects.
   */
  private Result addThriftRegionInfoQualifierIfNeeded(Result tentativeResult) {
    //TODO: Thrift has some problem serializing HRegionInfo. Since this method is only
    // for C++ client, I temporarily disable it. After the problem is fixed we should
    // remove the flag.
    if (HConstants.DISABLE_THRIFT_REGION_INFO_QUALIFIER) {
      return tentativeResult;
    }

    // Get the serialized HRegionInfo object
    byte[] value = tentativeResult.searchValue(HConstants.CATALOG_FAMILY,
      HConstants.REGIONINFO_QUALIFIER);
    // If the value exists, then we need to do something, otherwise, we return
    // the result untouched.
    if (value != null && value.length > 0) {
      try {
        // Get the Writable-serialized HRegionInfo object.
        HRegionInfo hri = (HRegionInfo) Writables.getWritable(value,
          new HRegionInfo());
        byte[] thriftSerializedHri =
          Bytes.writeThriftBytes(hri, HRegionInfo.class);
        // Create the KV with the thrift-serialized HRegionInfo
        List<KeyValue> kvList = tentativeResult.getKvs();
        KeyValue kv = new KeyValue(kvList.get(0).getRow(),
          HConstants.CATALOG_FAMILY, HConstants.THRIFT_REGIONINFO_QUALIFIER,
          thriftSerializedHri);
        kvList.add(kv);
        return new Result(kvList);
      } catch (Exception e) {
        // If failed, log the mistake and returns the original result
        LOG.error("Thrift Serialization of the HRegionInfo object failed!");
        e.printStackTrace();
      }
    }
    return tentativeResult;
  }

  @Override
  public void flushRegion(byte[] regionName)
      throws ThriftHBaseException {
    try {
      server.flushRegion(regionName);
    } catch (Exception e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void flushRegion(byte[] regionName, long ifOlderThanTS)
      throws ThriftHBaseException {
    try {
      server.flushRegion(regionName, ifOlderThanTS);
    } catch (IllegalArgumentException e) {
      throw new ThriftHBaseException(e);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public long getLastFlushTime(byte[] regionName) {
    return server.getLastFlushTime(regionName);
  }

  @Override
  public Map<byte[], Long> getLastFlushTimes() {
    MapWritable mapWritable = server.getLastFlushTimes();
    Map<byte[], Long> map = new HashMap<byte[], Long>();

    for (Entry<Writable, Writable> e : mapWritable.entrySet()) {
      map.put(((BytesWritable) e.getKey()).getBytes(),
          ((LongWritable) e.getValue()).get());
    }
    return map;
  }

  @Override
  public long getCurrentTimeMillis() {
    return server.getCurrentTimeMillis();
  }

  @Override
  public long getStartCode() {
    return server.getStartCode();
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName, byte[] columnFamily)
      throws ThriftHBaseException {
    try {
      return server.getStoreFileList(regionName, columnFamily);
    } catch (IllegalArgumentException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public List<String> getStoreFileListForColumnFamilies(byte[] regionName,
      List<byte[]> columnFamilies) throws ThriftHBaseException {
    try {
      byte[][] columnFamiliesArray = new byte[columnFamilies.size()][];
      return server.getStoreFileList(regionName, columnFamilies.toArray(columnFamiliesArray));
    } catch (IllegalArgumentException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public List<String> getStoreFileListForAllColumnFamilies(byte[] regionName)
      throws ThriftHBaseException {
    try {
      return server.getStoreFileList(regionName);
    } catch (IllegalArgumentException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public List<String> getHLogsList(boolean rollCurrentHLog)
      throws ThriftHBaseException {
    try {
      return server.getHLogsList(rollCurrentHLog);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public Result get(byte[] regionName, Get get) throws ThriftHBaseException {
    try {
      return server.get(regionName, get);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public ListenableFuture<Result> getAsync(byte[] regionName, Get get) {
    try {
      return Futures.immediateFuture(get(regionName, get));
    } catch (ThriftHBaseException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public List<Result> getRows(byte[] regionName, List<Get> gets)
      throws ThriftHBaseException {
    try {
      List<Result> resultList = new ArrayList<>();
      Collections.addAll(resultList, server.get(regionName, gets));
      return resultList;
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public boolean exists(byte[] regionName, Get get)
      throws ThriftHBaseException {
    try {
      return server.exists(regionName, get);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }


  @Override
  public void put(byte[] regionName, Put put) throws ThriftHBaseException {
    try {
      server.put(regionName, put);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public int putRows(byte[] regionName, List<Put> puts)
      throws ThriftHBaseException {
    try {
      return server.put(regionName, puts);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void processDelete(byte[] regionName, Delete delete)
      throws ThriftHBaseException {
    try {
      server.delete(regionName, delete);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public ListenableFuture<Void> deleteAsync(final byte[] regionName, final Delete delete) {
    try {
      processDelete(regionName, delete);
      return Futures.immediateFuture(null);
    } catch (ThriftHBaseException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public int processListOfDeletes(byte[] regionName, List<Delete> deletes)
      throws ThriftHBaseException {
    try {
      return server.delete(regionName, deletes);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, byte[] value, Put put) throws ThriftHBaseException {
    try {
      return server.checkAndPut(regionName, row, family, qualifier, value, put);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, byte[] value, Delete delete)
      throws ThriftHBaseException {
    try {
      return server.checkAndDelete(regionName, row, family, qualifier, value,
          delete);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public long incrementColumnValue(byte[] regionName, byte[] row,
      byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
      throws ThriftHBaseException {
    try {
      return server.incrementColumnValue(regionName, row, family, qualifier, amount, writeToWAL);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public long openScanner(byte[] regionName, Scan scan)
      throws ThriftHBaseException {
    try {
      return server.openScanner(regionName, scan);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void mutateRow(byte[] regionName, TRowMutations arm)
      throws ThriftHBaseException {
    try {
      server.mutateRow(regionName, RowMutations.Builder.createFromTRowMutations(arm));
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void mutateRows(byte[] regionName, List<TRowMutations> armList)
      throws ThriftHBaseException {
    try {
      List<RowMutations> rowMutations = new ArrayList<>();
      for (TRowMutations mutation : armList) {
        rowMutations.add(RowMutations.Builder.createFromTRowMutations(mutation));
      }
      server.mutateRow(regionName, rowMutations);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public ListenableFuture<Void> mutateRowAsync(byte[] regionName, TRowMutations arm) {
    try {
      mutateRow(regionName, arm);
      return Futures.immediateFuture(null);
    } catch (ThriftHBaseException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  @Deprecated
  public Result next(long scannerId) throws ThriftHBaseException {
    try {
      return server.next(scannerId);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public List<Result> nextRows(long scannerId, int numberOfRows)
      throws ThriftHBaseException {
    try {
      Result[] result = server.nextInternal(scannerId, numberOfRows);
      List<Result> resultList = new ArrayList<>(result.length);
      for (int i = 0; i < result.length; i ++) {
        resultList.add(this.addThriftRegionInfoQualifierIfNeeded(result[i]));
      }
      return resultList;
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void close(long scannerId) throws ThriftHBaseException {
    try {
      server.close(scannerId);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public long lockRow(byte[] regionName, byte[] row)
      throws ThriftHBaseException {
    try {
      return server.lockRow(regionName, row);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public ListenableFuture<RowLock> lockRowAsync(byte[] regionName, byte[] row) {
    try {
      long lockId = lockRow(regionName, row);
      return Futures.immediateFuture(new RowLock(row, lockId));
    } catch (ThriftHBaseException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public void unlockRow(byte[] regionName, long lockId)
      throws ThriftHBaseException {
    try {
      server.unlockRow(regionName, lockId);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public ListenableFuture<Void> unlockRowAsync(byte[] regionName, long lockId) {
    try {
      unlockRow(regionName, lockId);
      return Futures.immediateFuture(null);
    } catch (ThriftHBaseException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public List<HRegionInfo> getRegionsAssignment() throws ThriftHBaseException {
    try {
      return Arrays.asList(server.getRegionsAssignment());
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public HServerInfo getHServerInfo() throws ThriftHBaseException {
    try {
      return server.getHServerInfo();
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public TMultiResponse multiAction(MultiAction multi)
      throws ThriftHBaseException {
    try {
      return TMultiResponse.Builder.createFromMultiResponse(server
          .multiAction(multi));
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public MultiPutResponse multiPut(MultiPut puts) throws ThriftHBaseException {
    try {
      return server.multiPut(puts);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void bulkLoadHFile(String hfilePath, byte[] regionName,
      byte[] familyName) throws ThriftHBaseException {
    try {
      server.bulkLoadHFile(hfilePath, regionName, familyName);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void bulkLoadHFile(String hfilePath, byte[] regionName,
      byte[] familyName, boolean assignSeqNum) throws ThriftHBaseException {
    try {
      server.bulkLoadHFile(hfilePath, regionName, familyName, assignSeqNum);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void closeRegion(HRegionInfo hri, boolean reportWhenCompleted)
      throws ThriftHBaseException {
    try {
      server.closeRegion(hri, reportWhenCompleted);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public int updateFavoredNodes(AssignmentPlan plan) throws ThriftHBaseException {
    try {
      return server.updateFavoredNodes(plan);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public void updateConfiguration() {
    server.updateConfiguration();
  }

  @Override
  public void stop(String why) {
    server.stop(why);
  }

  @Override
  public String getStopReason() {
    return server.getStopReason();
  }

  @Override
  public void setNumHDFSQuorumReadThreads(int maxThreads) {
    server.setNumHDFSQuorumReadThreads(maxThreads);
  }

  @Override
  public void setHDFSQuorumReadTimeoutMillis(long timeoutMillis) {
    server.setHDFSQuorumReadTimeoutMillis(timeoutMillis);
  }

  @Override
  public boolean isStopped() {
    return server.isStopped();
  }

  @Override
  public void stopForRestart() {
    server.stopForRestart();
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }

  @Override
  public String getConfProperty(String paramName) throws ThriftHBaseException {
    return server.getConfProperty(paramName);
  }

  @Override
  public List<Bucket> getHistogram(byte[] regionName)
      throws ThriftHBaseException {
    try {
      List<Bucket> buckets = server.getHistogram(regionName);
      if (buckets == null) return new ArrayList<Bucket>();
      return buckets;
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public List<Bucket> getHistogramForStore(byte[] regionName, byte[] family)
      throws ThriftHBaseException {
    try {
      List<Bucket> buckets = server.getHistogramForStore(regionName, family);
      if (buckets == null) return new ArrayList<Bucket>();
      return buckets;
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }

  @Override
  public byte[] callEndpoint(String epName, String methodName,
      final byte[] regionName, final byte[] startRow, final byte[] stopRow)
      throws ThriftHBaseException {
    return endpointServer.callEndpoint(epName, methodName, regionName,
        startRow, stopRow);
  }

  @Override
  public List<List<Bucket>> getHistograms(List<byte[]> regionNames)
    throws ThriftHBaseException {
    return this.getHistograms(regionNames);
  }

  @Override
  public HRegionLocation getLocation(byte[] table, byte[] row, boolean reload)
    throws ThriftHBaseException {
    try {
      return server.getLocation(table, row, reload);
    } catch (IOException e) {
      throw new ThriftHBaseException(e);
    }
  }
}

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
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.ipc.ScannerResult;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.master.AssignmentPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This is just a wrapper around {@link HRegionServer}
 *
 */
public class ThriftHRegionServer implements ThriftHRegionInterface.Async {
  private static Log LOG = LogFactory.getLog(ThriftHRegionServer.class);

  private HRegionServer server;

  public ThriftHRegionServer(HRegionServer server) {
    this.server = server;
    final Configuration configuration = server.getConfiguration();

    // Current default assumes 80% writes to 20% reads.
    int readHandlers = configuration.getInt("hbase.regionserver.handler.read.count",
        configuration.getInt("hbase.regionserver.handler.count", 300) / 5 );
    int writeHandlers = configuration.getInt("hbase.regionserver.handler.write.count",
        configuration.getInt("hbase.regionserver.handler.count", 300) - readHandlers );

    writeService = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(writeHandlers,
            new DaemonThreadFactory("HBase-Write-Handler-")));

    readService = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(readHandlers,
            new DaemonThreadFactory("HBase-Read-Handler-")));
  }

  private final ListeningExecutorService writeService;


  private final ListeningExecutorService readService;


  @Override
  public ListenableFuture<HRegionInfo> getRegionInfo(final byte[] regionName) {
    return readService.submit(new Callable<HRegionInfo>() {
      @Override public HRegionInfo call() throws Exception {
        try {
          HRegionInfo r = server.getRegionInfo(regionName);
          LOG.debug("Printing the result of getClosestRowOrBefore : " + r);
          return r;
        } catch (NotServingRegionException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Result> getClosestRowBefore(final byte[] regionName, final byte[] row,
      final byte[] family) {
    return readService.submit(new Callable<Result>(){

      @Override public Result call() throws Exception {
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
    });

  }

  @Override
  public ListenableFuture<Void> flushRegion(final byte[] regionName) {
    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.flushRegion(regionName);
        } catch (Exception e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> flushRegion(final byte[] regionName, final long ifOlderThanTS) {
    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.flushRegion(regionName, ifOlderThanTS);
        } catch (IllegalArgumentException e) {
          throw new ThriftHBaseException(e);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Long> getLastFlushTime(final byte[] regionName) {
    return readService.submit(new Callable<Long>() {
      @Override public Long call() throws Exception {
        return server.getLastFlushTime(regionName);
      }
    });
  }

  @Override
  public ListenableFuture<Map<byte[], Long>> getLastFlushTimes() {
    return readService.submit(new Callable<Map<byte[], Long>>() {
      @Override public Map<byte[], Long> call() throws Exception {
        MapWritable mapWritable = server.getLastFlushTimes();
        Map<byte[], Long> map = new HashMap<>();

        for (Entry<Writable, Writable> e : mapWritable.entrySet()) {
          map.put(((BytesWritable) e.getKey()).getBytes(),
              ((LongWritable) e.getValue()).get());
        }
        return map;
      }
    });
  }

  @Override
  public ListenableFuture<Long> getCurrentTimeMillis() {
    return readService.submit(new Callable<Long>() {
      @Override public Long call() throws Exception {
        return server.getCurrentTimeMillis();
      }
    });
  }

  @Override
  public ListenableFuture<Long> getStartCode() {
    return readService.submit(new Callable<Long>() {
      @Override public Long call() throws Exception {
        return server.getStartCode();
      }
    });
  }

  @Override
  public ListenableFuture<List<String>> getStoreFileList(final byte[] regionName, final byte[] columnFamily) {
    return readService.submit(new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        try {
          return server.getStoreFileList(regionName, columnFamily);
        } catch (IllegalArgumentException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<List<String>> getStoreFileListForColumnFamilies(final byte[] regionName,
      final List<byte[]> columnFamilies) {
    return readService.submit(new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        try {
          byte[][] columnFamiliesArray = new byte[columnFamilies.size()][];
          return server.getStoreFileList(regionName, columnFamilies.toArray(columnFamiliesArray));
        } catch (IllegalArgumentException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<List<String>> getStoreFileListForAllColumnFamilies(final byte[] regionName) {
    return readService.submit(new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        try {
          return server.getStoreFileList(regionName);
        } catch (IllegalArgumentException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<List<String>> getHLogsList(final boolean rollCurrentHLog) {
    return readService.submit(new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        try {
          return server.getHLogsList(rollCurrentHLog);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Result> get(final byte[] regionName, final Get get) {
    return readService.submit(new Callable<Result>() {
      @Override public Result call() throws Exception {
        try {
          return server.get(regionName, get);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }


  @Override
  public ListenableFuture<List<Result>> getRows(final byte[] regionName, final List<Get> gets) {
    return readService.submit(new Callable<List<Result>>() {
      @Override public List<Result> call() throws Exception {
        try {
          List<Result> resultList = new ArrayList<>();
          Collections.addAll(resultList, server.get(regionName, gets));
          return resultList;
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Boolean> exists(final byte[] regionName, final Get get){
    return readService.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        try {
          return server.exists(regionName, get);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }


  @Override
  public ListenableFuture<Void> put(final byte[] regionName, final Put put) {

    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.put(regionName, put);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Integer> putRows(final byte[] regionName, final List<Put> puts) {
    return writeService.submit(new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        try {
          return server.put(regionName, puts);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Void> processDelete(final byte[] regionName, final Delete delete)  {
    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.delete(regionName, delete);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Integer> processListOfDeletes(final byte[] regionName, final List<Delete> deletes) {
    return writeService.submit(new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        try {
          return server.delete(regionName, deletes);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Boolean> checkAndPut(final byte[] regionName, final byte[] row, final byte[] family,
      final byte[] qualifier, final byte[] value, final Put put) {

    return writeService.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        try {
          return server.checkAndPut(regionName, row, family, qualifier, value, put);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Boolean> checkAndDelete(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final byte[] value, final Delete delete) {

    return writeService.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        try {
          return server.checkAndDelete(regionName, row, family, qualifier, value,
              delete);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Long> incrementColumnValue(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final long amount, final boolean writeToWAL) {

    return writeService.submit(new Callable<Long>() {
      @Override public Long call() throws Exception {
        try {
          return server.incrementColumnValue(regionName, row, family, qualifier, amount, writeToWAL);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Long> openScanner(final byte[] regionName, final Scan scan) {
    return readService.submit(new Callable<Long>() {
      @Override public Long call() throws Exception {
        try {
          return server.openScanner(regionName, scan);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Void> mutateRow(final byte[] regionName, final TRowMutations arm) {

    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.mutateRow(regionName, RowMutations.Builder.createFromTRowMutations(arm));
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> mutateRows(final byte[] regionName, final List<TRowMutations> armList) {
    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          List<RowMutations> rowMutations = new ArrayList<>();
          for (TRowMutations mutation : armList) {
            rowMutations.add(RowMutations.Builder.createFromTRowMutations(mutation));
          }
          server.mutateRow(regionName, rowMutations);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  @Deprecated
  public ListenableFuture<Result> next(final long scannerId) {
    return readService.submit(new Callable<Result>() {
      @Override public Result call() throws Exception {
        try {
          return server.next(scannerId);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<List<Result>> nextRows(final long scannerId, final int numberOfRows) {
    return readService.submit(new Callable<List<Result>>() {
      @Override public List<Result> call() throws Exception {
        try {
          Result[] result = server.nextInternal(scannerId, numberOfRows);
          List<Result> resultList = new ArrayList<>(result.length);
          for (int i = 0; i < result.length; i ++) {
            resultList.add(addThriftRegionInfoQualifierIfNeeded(result[i]));
          }
          return resultList;
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Void> close(final long scannerId) {
    return readService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.close(scannerId);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<RowLock> lockRow(final byte[] regionName, final byte[] row) {
    return readService.submit(new Callable<RowLock>() {
      @Override public RowLock call() throws Exception {
        try {
          return new RowLock(row, server.lockRow(regionName, row));
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Void> unlockRow(final byte[] regionName, final long lockId) {
    return readService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.unlockRow(regionName, lockId);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<List<HRegionInfo>> getRegionsAssignment() {

    return readService.submit(new Callable<List<HRegionInfo>>() {
      @Override public List<HRegionInfo> call() throws Exception {
        try {
          return Arrays.asList(server.getRegionsAssignment());
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<HServerInfo> getHServerInfo() {
    return readService.submit(new Callable<HServerInfo>() {
      @Override public HServerInfo call() throws Exception {
        try {
          return server.getHServerInfo();
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<TMultiResponse> multiAction(final MultiAction multi) {
    return writeService.submit(new Callable<TMultiResponse>() {
      @Override public TMultiResponse call() throws Exception {
        try {
          return TMultiResponse.Builder.createFromMultiResponse(server
              .multiAction(multi));
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<MultiPutResponse> multiPut(final MultiPut puts) {

    return writeService.submit(new Callable<MultiPutResponse>() {
      @Override public MultiPutResponse call() throws Exception {
        try {
          return server.multiPut(puts);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Void> bulkLoadHFile(final String hfilePath, final byte[] regionName,
      final byte[] familyName) {
    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.bulkLoadHFile(hfilePath, regionName, familyName);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> bulkLoadHFile(final String hfilePath, final byte[] regionName,
      final byte[] familyName, final boolean assignSeqNum) {
    return writeService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.bulkLoadHFile(hfilePath, regionName, familyName, assignSeqNum);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> closeRegion(final HRegionInfo hri, final boolean reportWhenCompleted) {

    return readService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        try {
          server.closeRegion(hri, reportWhenCompleted);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Integer> updateFavoredNodes(final AssignmentPlan plan) {
    return readService.submit(new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        try {
          return server.updateFavoredNodes(plan);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<Void> updateConfiguration() {
    return readService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        server.updateConfiguration();
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> stop(final String why) {
    return readService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        server.stop(why);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<String> getStopReason() {
    return readService.submit(new Callable<String>() {
      @Override public String call() throws Exception {
        return server.getStopReason();
      }
    });
  }

  @Override
  public ListenableFuture<Void> setNumHDFSQuorumReadThreads(final int maxThreads) {
    return readService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        server.setNumHDFSQuorumReadThreads(maxThreads);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> setHDFSQuorumReadTimeoutMillis(long timeoutMillis) {
    server.setHDFSQuorumReadTimeoutMillis(timeoutMillis);
    return null;

  }

  @Override
  public ListenableFuture<Boolean> isStopped() {
    return readService.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        return server.isStopped();
      }
    });
  }

  @Override
  public ListenableFuture<Void> stopForRestart() {

    return readService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        server.stopForRestart();
        return null;
      }
    });
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }

  @Override
  public ListenableFuture<String> getConfProperty(final String paramName) {
    return readService.submit(new Callable<String>() {
      @Override public String call() throws Exception {
        return server.getConfProperty(paramName);
      }
    });
  }

  @Override
  public ListenableFuture<List<Bucket>> getHistogram(final byte[] regionName) {
    return readService.submit(new Callable<List<Bucket>>() {
      @Override public List<Bucket> call() throws Exception {
        try {
          List<Bucket> buckets = server.getHistogram(regionName);
          if (buckets == null) return new ArrayList<Bucket>();
          return buckets;
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<List<Bucket>> getHistogramForStore(final byte[] regionName, final byte[] family) {
    return readService.submit(new Callable<List<Bucket>>() {
      @Override public List<Bucket> call() throws Exception {
        try {
          List<Bucket> buckets = server.getHistogramForStore(regionName, family);
          if (buckets == null) return new ArrayList<Bucket>();
          return buckets;
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<byte[]> callEndpoint(final String epName, final String methodName,
      final List<byte[]> params, final byte[] regionName, final byte[] startRow,
      final byte[] stopRow) {

    return readService.submit(new Callable<byte[]>() {
      @Override public byte[] call() throws Exception {
        try {
          return server.callEndpoint(epName, methodName, params, regionName,
              startRow, stopRow);
        } catch (IOException ioe) {
          throw new ThriftHBaseException(ioe);
        }
      }
    });
  }

  @Override
  public ListenableFuture<List<List<Bucket>>> getHistograms(final List<byte[]> regionNames) {
    return readService.submit(new Callable<List<List<Bucket>>>() {
      @Override public List<List<Bucket>> call() throws Exception {
        try {
          return server.getHistograms(regionNames);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<HRegionLocation> getLocation(
      final byte[] table, final byte[] row, final boolean reload)  {

    return readService.submit(new Callable<HRegionLocation>() {
      @Override public HRegionLocation call() throws Exception {
        try {
          return server.getLocation(table, row, reload);
        } catch (IOException e) {
          throw new ThriftHBaseException(e);
        }
      }
    });
  }

  @Override
  public ListenableFuture<ScannerResult> scanOpen(
      final byte[] regionName, final Scan scan, final int numberOfRows) {

    return readService.submit(new Callable<ScannerResult>() {
      @Override public ScannerResult call() throws Exception {
        return server.scanOpen(regionName, scan, numberOfRows);
      }
    });
  }

  @Override
  public ListenableFuture<ScannerResult> scanNext(final long id, final int numberOfRows) {

    return readService.submit(new Callable<ScannerResult>() {
      @Override public ScannerResult call() throws Exception {
        return server.scanNext(id, numberOfRows);
      }
    });
  }

  @Override
  public ListenableFuture<Boolean> scanClose(final long id) {
    return readService.submit(new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        return server.scanClose(id);
      }
    });
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
}

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
package org.apache.hadoop.hbase.ipc.thrift;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TRowMutations;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HBaseServer.Call;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.ipc.ScannerResult;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.master.AssignmentPlan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import com.facebook.nifty.header.transport.THeaderTransport;
import com.facebook.swift.service.RuntimeTApplicationException;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The client should use this class to communicate to the server via thrift
 *
 */
public class HBaseToThriftAdapter implements HRegionInterface {
  public static final Log LOG = LogFactory.getLog(HBaseToThriftAdapter.class);
  public ThriftHRegionInterface connection;
  public ThriftClientManager clientManager;
  private InetSocketAddress addr;
  private Configuration conf;
  private Class<? extends ThriftClientInterface> clazz;
  private HBaseRPCOptions options;
  private boolean useHeaderProtocol;
  // start time of the call on the client side
  private long startCallTimestamp;

  public HBaseToThriftAdapter(ThriftClientInterface connection,
      ThriftClientManager clientManager,
      InetSocketAddress addr, Configuration conf,
      Class<? extends ThriftClientInterface> clazz,
      HBaseRPCOptions options) {
    this.connection = (ThriftHRegionInterface)connection;
    this.clientManager = clientManager;
    this.addr = addr;
    this.conf = conf;
    this.clazz = clazz;
    this.options = options;
    this.useHeaderProtocol = conf.getBoolean(HConstants.USE_HEADER_PROTOCOL,
      HConstants.DEFAULT_USE_HEADER_PROTOCOL);
    this.startCallTimestamp = -1;
  }

  /**
   * Call this function as demonstrated in
   * {@link HBaseToThriftAdapter#getRegionInfo(byte[])}. This function when
   * called with the captured exception, will perform the appropriate action
   * depending upon what exception is thrown. Any unknown runtime exception
   * thrown by the swift or underlying libraries suggest that we refresh the
   * connection and re-throw the exception.
   *
   * @param e : The captured exception.
   */
  public void refreshConnectionAndThrowIOException(Exception e)
      throws IOException {
    if (e instanceof TApplicationException) {
      throw new RuntimeException(e);
    } else if (e instanceof RuntimeTApplicationException) {
      throw new RuntimeException(e);
    } else {
      //TODO: creating a new connection is unnecessary.
      // We should replace it with cleanUpConnection later
      Pair<ThriftClientInterface, ThriftClientManager> interfaceAndManager = HBaseThriftRPC
          .refreshConnection(this.addr, this.conf, this.connection, this.clazz);
      this.connection = (ThriftHRegionInterface) interfaceAndManager.getFirst();
      this.clientManager = interfaceAndManager.getSecond();
      throw new IOException(e);
    }
  }

  /**
   * In contrast to refreshConnectionAndThrowIOException(), it tries to clean
   * up failed connection from the pool without creating new ones, because that's
   * unnecessary.
   *
   * @param e
   * @throws IOException
   */
  public void cleanUpServerConnection(Exception e) throws IOException {
    if (e instanceof TApplicationException) {
      throw new RuntimeException(e);
    } else if (e instanceof RuntimeTApplicationException) {
      throw new RuntimeException(e);
    } else {
      HBaseThriftRPC.cleanUpConnection(this.addr, this.conf, this.connection, this.clazz);
      this.connection = null;
      this.clientManager = null;
    }
  }

  private void refreshConnectionAndThrowRuntimeException(
      Exception e) {
    try {
      refreshConnectionAndThrowIOException(e);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  /**
   * Send some data about the call from the client side to the server side
   *
   * @throws Exception
   */
  public void setHeader() throws Exception {
    TProtocol outputProtocol = clientManager.getOutputProtocol(connection);
    TTransport outputTransport = outputProtocol.getTransport();
    if (outputTransport instanceof THeaderTransport) {
      THeaderTransport headerTransport = (THeaderTransport) outputTransport;
      if (options != null && options.getRequestProfiling()) {
        Call call = new Call(options);
        String stringData = Bytes
            .writeThriftBytesAndGetString(call, Call.class);
        headerTransport.setHeader(HConstants.THRIFT_HEADER_FROM_CLIENT,
            stringData);
      }
    } else {
      LOG.error("output transport for client was not THeaderTransport, client cannot send headers");
    }
  }

  private void preProcess() {
    this.startCallTimestamp = EnvironmentEdgeManager.currentTimeMillis();
    if (this.connection == null || clientManager == null) {
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception e) {
          LOG.error("Could not close connection : " + connection, e);
        }
      }
      try {
        Pair<ThriftClientInterface, ThriftClientManager> clientAndManager = HBaseThriftRPC
            .getClientWithoutWrapper(addr, conf, clazz);
        this.connection = (ThriftHRegionInterface) clientAndManager.getFirst();
        this.clientManager = clientAndManager.getSecond();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    if (useHeaderProtocol) {
      try {
        setHeader();
      } catch (Exception e) {
        LOG.error("Header could not be sent", e);
      }
    }
  }

  /**
   * Read data that the server has sent to the client
   * TODO: test how it works with async calls
   */
  private void readHeader() {
    TTransport inputTransport = clientManager.getInputProtocol(connection)
        .getTransport();
    TTransport outputTransport = clientManager.getOutputProtocol(connection)
        .getTransport();
    if (inputTransport instanceof THeaderTransport) {
      THeaderTransport headerTransport = (THeaderTransport) outputTransport;
      headerTransport.clearHeaders();
      String dataString = headerTransport.getReadHeaders().get(HConstants.THRIFT_HEADER_FROM_SERVER);
      if (dataString != null) {
        byte[] dataBytes = Bytes.string64ToBytes(dataString);
        try {
          Call call = Bytes.readThriftBytes(dataBytes, Call.class);
          ProfilingData pd = call.getProfilingData();
          pd.addLong(ProfilingData.CLIENT_NETWORK_LATENCY_MS,
              EnvironmentEdgeManager.currentTimeMillis()
                  - this.startCallTimestamp);
          this.options.profilingResult = pd;
        } catch (Exception e) {
          LOG.error("data deserialization didn't succeed", e);
        }
      }
    } else {
      LOG.error("input transport was not THeaderTransport, client cannot read headers");
    }
  }

  public void postProcess() {
    if (this.clientManager != null && this.connection != null) {
      try {
        if (this.useHeaderProtocol) {
          readHeader();
        }
        HBaseThriftRPC.putBackClient(this.connection, this.addr, this.conf, this.clazz);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    this.connection = null;
    this.clientManager = null;
  }

  private void handleIOException(Exception e) throws IOException {
    if (e instanceof IOException) {
      throw (IOException) e;
    }
  }

  private void handleNotServingRegionException(Exception e)
    throws NotServingRegionException {
    if (e instanceof NotServingRegionException) {
      throw (NotServingRegionException) e;
    }
  }

  private void handleIllegalArgumentException(Exception e)
    throws IllegalArgumentException {
    if (e instanceof IllegalArgumentException) {
      throw (IllegalArgumentException) e;
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2)
      throws IOException {
    throw new UnsupportedOperationException("Method is not supported for thrift");
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1)
      throws VersionIncompatible, IOException {
    throw new UnsupportedOperationException("Method is not supported for thrift");
  }

  @Override
  public void stopForRestart() {
    preProcess();
    try {
      connection.stopForRestart();
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public boolean isStopped() {
    preProcess();
    try {
      return connection.isStopped();
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return false;
    } finally {
      postProcess();
    }
  }

  @Override
  public void close() throws Exception {
    clientManager = null;
    if (this.connection != null) {
      this.connection.close();
      this.connection = null;
    }
  }

  @Override
  public HRegionInfo getRegionInfo(byte[] regionName)
      throws NotServingRegionException {
    preProcess();
    try {
      return connection.getRegionInfo(regionName);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleNotServingRegionException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public Result getClosestRowBefore(byte[] regionName, byte[] row, byte[] family)
      throws IOException {
    preProcess();
    try {
      Result r = connection.getClosestRowBefore(regionName, row, family);
      if (r.isSentinelResult()) {
        return null;
      } else {
        return r;
      }
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  public ListenableFuture<Result> getClosestRowBeforeAsync(byte[] regionName, byte[] row, byte[] family) {
    preProcess();
    return connection.getClosestRowBeforeAsync(regionName, row, family);
  }

  // TODO: we will decide whether to remove it from HRegionInterface in the future
  @Override
  public HRegion[] getOnlineRegionsAsArray() {
    LOG.error("Intentionally not implemented because it's only called internally.");
    return null;
  }

  @Override
  public void flushRegion(byte[] regionName) throws IllegalArgumentException,
      IOException {
    preProcess();
    try {
      connection.flushRegion(regionName);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIllegalArgumentException(e);
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public void flushRegion(byte[] regionName, long ifOlderThanTS)
      throws IllegalArgumentException, IOException {
    preProcess();
    try {
      connection.flushRegion(regionName, ifOlderThanTS);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIllegalArgumentException(e);
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public long getLastFlushTime(byte[] regionName) {
    preProcess();
    try {
      return connection.getLastFlushTime(regionName);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public MapWritable getLastFlushTimes() {
    preProcess();
    try {
      Map<byte[], Long> map = connection.getLastFlushTimes();
      MapWritable writableMap = new MapWritable();
      for (Entry<byte[], Long> e : map.entrySet()) {
        writableMap.put(new BytesWritable(e.getKey()),
            new LongWritable(e.getValue()));
      }
      return writableMap;
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      throw new RuntimeException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public long getCurrentTimeMillis() {
    preProcess();
    try {
      return connection.getCurrentTimeMillis();
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public long getStartCode() {
    preProcess();
    try {
      return connection.getStartCode();
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName, byte[] columnFamily)
      throws IllegalArgumentException {
    preProcess();
    try {
      return connection.getStoreFileList(regionName, columnFamily);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIllegalArgumentException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName,
      byte[][] columnFamilies) throws IllegalArgumentException {
    preProcess();
    try {
      List<byte[]> columnFamiliesList = new ArrayList<>();
      Collections.addAll(columnFamiliesList, columnFamilies);
      return connection.getStoreFileListForColumnFamilies(regionName,
          columnFamiliesList);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIllegalArgumentException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName)
      throws IllegalArgumentException {
    preProcess();
    try {
      return connection.getStoreFileListForAllColumnFamilies(regionName);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIllegalArgumentException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public List<String> getHLogsList(boolean rollCurrentHLog) throws IOException {
    preProcess();
    try {
      return connection.getHLogsList(rollCurrentHLog);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public Result get(byte[] regionName, Get get) throws IOException {
    preProcess();
    try {
      return connection.get(regionName, get);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public Result[] get(byte[] regionName, List<Get> gets) throws IOException {
    preProcess();
    try {
      List<Result> listOfResults = connection.getRows(regionName, gets);
      return listOfResults.toArray(new Result[listOfResults.size()]);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  public ListenableFuture<Result> getAsync(byte[] regionName, Get get) {
    preProcess();
    return connection.getAsync(regionName, get);
  }

  @Override
  public boolean exists(byte[] regionName, Get get) throws IOException {
    preProcess();
    try {
      return connection.exists(regionName, get);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return false;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return false;
    } finally {
      postProcess();
    }
  }

  @Override
  public void put(byte[] regionName, Put put) throws IOException {
    preProcess();
    try {
      connection.put(regionName, put);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public int put(byte[] regionName, List<Put> puts) throws IOException {
    preProcess();
    try {
      return connection.putRows(regionName, puts);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return -1;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public void delete(byte[] regionName, Delete delete) throws IOException {
    preProcess();
    try {
      connection.processDelete(regionName, delete);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  public ListenableFuture<Void> deleteAsync(final byte[] regionName, final Delete delete) {
    preProcess();
    return connection.deleteAsync(regionName, delete);
  }

  @Override
  public int delete(byte[] regionName, List<Delete> deletes)
      throws IOException {
    preProcess();
    try {
      return connection.processListOfDeletes(regionName, deletes);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return -1;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, byte[] value, Put put) throws IOException {
    preProcess();
    try {
      return connection.checkAndPut(regionName, row, family, qualifier, value, put);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return false;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return false;
    } finally {
      postProcess();
    }
  }

  @Override
  public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family,
      byte[] qualifier, byte[] value, Delete delete) throws IOException {
    preProcess();
    try {
      return connection.checkAndDelete(regionName, row, family, qualifier, value, delete);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return false;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return false;
    } finally {
      postProcess();
    }
  }

  @Override
  public long incrementColumnValue(byte[] regionName, byte[] row,
      byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
      throws IOException {
    preProcess();
    try {
      return connection.incrementColumnValue(regionName, row, family, qualifier, amount, writeToWAL);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return -1;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public long openScanner(byte[] regionName, Scan scan) throws IOException {
    preProcess();
    try {
      return connection.openScanner(regionName, scan);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return -1;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public void mutateRow(byte[] regionName, RowMutations arm)
      throws IOException {
    preProcess();
    try {
      connection.mutateRow(regionName, TRowMutations.Builder.createFromRowMutations(arm));
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  public ListenableFuture<Void> mutateRowAsync(byte[] regionName, RowMutations arm) {
    preProcess();
    try {
      return connection.mutateRowAsync(regionName, TRowMutations.Builder.createFromRowMutations(arm));
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public void mutateRow(byte[] regionName, List<RowMutations> armList)
      throws IOException {
    preProcess();
    try {
      List<TRowMutations> listOfMutations = new ArrayList<>();
      for (RowMutations mutation : armList) {
        listOfMutations.add(TRowMutations.Builder.createFromRowMutations(mutation));
      }
      connection.mutateRows(regionName, listOfMutations);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  @Deprecated
  public Result next(long scannerId) throws IOException {
    preProcess();
    try {
      return connection.next(scannerId);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  /**
   * Maps the new terminal condition of the Result stream back to the original
   * format.
   * {@link Result#SENTINEL_RESULT_ARRAY} as the return value to
   * {@link this#next(long, int)} represents the terminal condition to the end
   * of scan.
   * Previously, null used to represent the same.
   * This method maps the new format to the old format to maintain backward
   * compatibility.
   * @param res
   * @return
   */
  public static Result[] validateResults(Result[] res) {
    if (res == null) return null;
    if (res.length == 1) {
      return res[0].isSentinelResult() ? null : res;
    }
    return res;
  }

  @Override
  public Result[] next(long scannerId, int numberOfRows) throws IOException {
    preProcess();
    try {
      List<Result> resultList = connection.nextRows(scannerId, numberOfRows);
      Result[] ret = resultList.toArray(new Result[resultList.size()]);
      return validateResults(ret);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public void close(long scannerId) throws IOException {
    preProcess();
    try {
      connection.close(scannerId);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public long lockRow(byte[] regionName, byte[] row) throws IOException {
    preProcess();
    try {
      return connection.lockRow(regionName, row);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return -1;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  public ListenableFuture<RowLock> lockRowAsync(byte[] regionName, byte[] row) {
    preProcess();
    return connection.lockRowAsync(regionName, row);
  }

  @Override
  public void unlockRow(byte[] regionName, long lockId) throws IOException {
    preProcess();
    try {
      connection.unlockRow(regionName, lockId);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  public ListenableFuture<Void> unlockRowAsync(byte[] regionName, long lockId) {
    preProcess();
    return connection.unlockRowAsync(regionName, lockId);
  }

  @Override
  public HRegionInfo[] getRegionsAssignment() throws IOException {
    preProcess();
    try {
      List<HRegionInfo> hRegionInfos = connection.getRegionsAssignment();
      return hRegionInfos.toArray(new HRegionInfo[hRegionInfos.size()]);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public HServerInfo getHServerInfo() throws IOException {
    preProcess();
    try {
      return connection.getHServerInfo();
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public MultiResponse multiAction(MultiAction multi) throws IOException {
    preProcess();
    try {
      return MultiResponse.Builder.createFromTMultiResponse(connection
          .multiAction(multi));
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public MultiPutResponse multiPut(MultiPut puts) throws IOException {
    preProcess();
    try {
      return connection.multiPut(puts);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public void bulkLoadHFile(String hfilePath, byte[] regionName,
      byte[] familyName) throws IOException {
    preProcess();
    try {
      connection.bulkLoadHFile(hfilePath, regionName, familyName);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public void bulkLoadHFile(String hfilePath, byte[] regionName,
      byte[] familyName, boolean assignSeqNum) throws IOException {
    preProcess();
    try {
      connection.bulkLoadHFile(hfilePath, regionName, familyName, assignSeqNum);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public void closeRegion(HRegionInfo hri, boolean reportWhenCompleted)
      throws IOException {
    preProcess();
    try {
      connection.closeRegion(hri, reportWhenCompleted);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public int updateFavoredNodes(AssignmentPlan plan) throws IOException {
    preProcess();
    try {
      return connection.updateFavoredNodes(plan);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return -1;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return -1;
    } finally {
      postProcess();
    }
  }

  @Override
  public void updateConfiguration() {
    preProcess();
    try {
      connection.updateConfiguration();
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public void stop(String why) {
    preProcess();
    try {
      connection.stop(why);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public String getStopReason() {
    preProcess();
    try {
      return connection.getStopReason();
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
      throw new RuntimeException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public void setNumHDFSQuorumReadThreads(int maxThreads) {
    preProcess();
    try {
      connection.setNumHDFSQuorumReadThreads(maxThreads);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public void setHDFSQuorumReadTimeoutMillis(long timeoutMillis) {
    preProcess();
    try {
      connection.setHDFSQuorumReadTimeoutMillis(timeoutMillis);
    } catch (Exception e) {
      refreshConnectionAndThrowRuntimeException(e);
    } finally {
      postProcess();
    }
  }

  @Override
  public String getConfProperty(String paramName) throws IOException {
    preProcess();
    try {
      return connection.getConfProperty(paramName);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return null;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public List<Bucket> getHistogram(byte[] regionName) throws IOException {
    preProcess();
    try {
       List<Bucket> buckets = connection.getHistogram(regionName);
      if (buckets.isEmpty()) {
        return null;
      }
      return buckets;
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return null;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public List<Bucket> getHistogramForStore(byte[] regionName, byte[] family)
      throws IOException {
    preProcess();
    try {
      List<Bucket> buckets =
          connection.getHistogramForStore(regionName, family);
      if (buckets.isEmpty()) {
        return null;
      }
      return buckets;
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return null;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public byte[] callEndpoint(String epName, String methodName,
      List<byte[]> params, byte[] regionName, byte[] startRow,
      byte[] stopRow) throws ThriftHBaseException {
    preProcess();
    try {
      return connection.callEndpoint(epName, methodName, params, regionName,
          startRow, stopRow);
    } finally {
      postProcess();
    }
  }

  @Override
  public List<List<Bucket>> getHistograms(List<byte[]> regionNames)
    throws IOException {
    preProcess();
    try {
      return connection.getHistograms(regionNames);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      return null;
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public HRegionLocation getLocation(byte[] table, byte[] row, boolean reload)
    throws IOException {
    preProcess();
    try {
      return connection.getLocation(table, row, reload);
    } catch (ThriftHBaseException te) {
      Exception e = te.getServerJavaException();
      handleIOException(e);
      LOG.warn("Unexpected Exception: " + e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      refreshConnectionAndThrowIOException(e);
      return null;
    } finally {
      postProcess();
    }
  }

  @Override
  public ScannerResult scanOpen(byte[] regionName, Scan scan, int numberOfRows)
      throws ThriftHBaseException {
    preProcess();
    try {
      return connection.scanOpen(regionName, scan, numberOfRows);
    } finally {
      postProcess();
    }
  }

  @Override
  public ScannerResult scanNext(long id, int numberOfRows)
      throws ThriftHBaseException {
    preProcess();
    try {
      return connection.scanNext(id, numberOfRows);
    } finally {
      postProcess();
    }
  }

  @Override
  public boolean scanClose(long id) throws ThriftHBaseException {
    preProcess();
    try {
      return connection.scanClose(id);
    } finally {
      postProcess();
    }
  }

}

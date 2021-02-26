/**
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
package org.apache.hadoop.hbase.thrift2.client;

import static org.apache.hadoop.hbase.thrift.Constants.HBASE_THRIFT_CLIENT_SCANNER_CACHING;
import static org.apache.hadoop.hbase.thrift.Constants.HBASE_THRIFT_CLIENT_SCANNER_CACHING_DEFAULT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.thrift2.ThriftUtilities;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.primitives.Booleans;

@InterfaceAudience.Private
public class ThriftTable implements Table {

  private TableName tableName;
  private Configuration conf;
  private TTransport tTransport;
  private THBaseService.Client client;
  private ByteBuffer tableNameInBytes;
  private int operationTimeout;

  private final int scannerCaching;

  public ThriftTable(TableName tableName, THBaseService.Client client, TTransport tTransport,
      Configuration conf) {
    this.tableName = tableName;
    this.tableNameInBytes = ByteBuffer.wrap(tableName.toBytes());
    this.conf = conf;
    this.tTransport = tTransport;
    this.client = client;
    this.scannerCaching = conf.getInt(HBASE_THRIFT_CLIENT_SCANNER_CACHING,
        HBASE_THRIFT_CLIENT_SCANNER_CACHING_DEFAULT);
    this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);


  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    try {
      TTableDescriptor tableDescriptor = client
          .getTableDescriptor(ThriftUtilities.tableNameFromHBase(tableName));
      return ThriftUtilities.tableDescriptorFromThrift(tableDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean exists(Get get) throws IOException {
    TGet tGet = ThriftUtilities.getFromHBase(get);
    try {
      return client.exists(tableNameInBytes, tGet);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    List<TGet> tGets = new ArrayList<>();
    for (Get get: gets) {
      tGets.add(ThriftUtilities.getFromHBase(get));
    }
    try {
      List<Boolean> results = client.existsAll(tableNameInBytes, tGets);
      return Booleans.toArray(results);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException {
    throw new IOException("Batch not supported in ThriftTable, use put(List<Put> puts), "
        + "get(List<Get> gets) or delete(List<Delete> deletes) respectively");


  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException {
    throw new IOException("BatchCallback not supported in ThriftTable, use put(List<Put> puts), "
        + "get(List<Get> gets) or delete(List<Delete> deletes) respectively");
  }

  @Override
  public Result get(Get get) throws IOException {
    TGet tGet = ThriftUtilities.getFromHBase(get);
    try {
      TResult tResult = client.get(tableNameInBytes, tGet);
      return ThriftUtilities.resultFromThrift(tResult);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    List<TGet> tGets = ThriftUtilities.getsFromHBase(gets);
    try {
      List<TResult> results = client.getMultiple(tableNameInBytes, tGets);
      return ThriftUtilities.resultsFromThrift(results);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  /**
   * A scanner to perform scan from thrift server
   * getScannerResults is used in this scanner
   */
  private class Scanner implements ResultScanner {
    protected TScan scan;
    protected Result lastResult = null;
    protected final Queue<Result> cache = new ArrayDeque<>();;


    public Scanner(Scan scan) throws IOException {
      if (scan.getBatch() > 0) {
        throw new IOException("Batch is not supported in Scanner");
      }
      if (scan.getCaching() <= 0) {
        scan.setCaching(scannerCaching);
      } else if (scan.getCaching() == 1 && scan.isReversed()){
        // for reverse scan, we need to pass the last row to the next scanner
        // we need caching number bigger than 1
        scan.setCaching(scan.getCaching() + 1);
      }
      this.scan = ThriftUtilities.scanFromHBase(scan);
    }


    @Override
    public Result next() throws IOException {
      if (cache.size() == 0) {
        setupNextScanner();
        try {
          List<TResult> tResults = client
              .getScannerResults(tableNameInBytes, scan, scan.getCaching());
          Result[] results = ThriftUtilities.resultsFromThrift(tResults);
          boolean firstKey = true;
          for (Result result : results) {
            // If it is a reverse scan, we use the last result's key as the startkey, since there is
            // no way to construct a closet rowkey smaller than the last result
            // So when the results return, we must rule out the first result, since it has already
            // returned to user.
            if (firstKey) {
              firstKey = false;
              if (scan.isReversed() && lastResult != null) {
                if (Bytes.equals(lastResult.getRow(), result.getRow())) {
                  continue;
                }
              }
            }
            cache.add(result);
            lastResult = result;
          }
        } catch (TException e) {
          throw new IOException(e);
        }
      }

      if (cache.size() > 0) {
        return cache.poll();
      } else {
        //scan finished
        return null;
      }
    }

    @Override
    public void close() {
    }

    @Override
    public boolean renewLease() {
      throw new RuntimeException("renewLease() not supported");
    }

    @Override
    public ScanMetrics getScanMetrics() {
      throw new RuntimeException("getScanMetrics() not supported");
    }

    private void setupNextScanner() {
      //if lastResult is null null, it means it is not the fist scan
      if (lastResult!= null) {
        byte[] lastRow = lastResult.getRow();
        if (scan.isReversed()) {
          //for reverse scan, we can't find the closet row before this row
          scan.setStartRow(lastRow);
        } else {
          scan.setStartRow(createClosestRowAfter(lastRow));
        }
      }
    }


    /**
     * Create the closest row after the specified row
     */
    protected byte[] createClosestRowAfter(byte[] row) {
      if (row == null) {
        throw new RuntimeException("The passed row is null");
      }
      return Arrays.copyOf(row, row.length + 1);
    }
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return new Scanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  @Override
  public void put(Put put) throws IOException {
    TPut tPut = ThriftUtilities.putFromHBase(put);
    try {
      client.put(tableNameInBytes, tPut);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    List<TPut> tPuts = ThriftUtilities.putsFromHBase(puts);
    try {
      client.putMultiple(tableNameInBytes, tPuts);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    TDelete tDelete = ThriftUtilities.deleteFromHBase(delete);
    try {
      client.deleteSingle(tableNameInBytes, tDelete);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    List<TDelete> tDeletes = ThriftUtilities.deletesFromHBase(deletes);
    try {
      client.deleteMultiple(tableNameInBytes, tDeletes);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  private class CheckAndMutateBuilderImpl implements CheckAndMutateBuilder {

    private final byte[] row;
    private final byte[] family;
    private byte[] qualifier;
    private CompareOperator op;
    private byte[] value;

    CheckAndMutateBuilderImpl(byte[] row, byte[] family) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.family = Preconditions.checkNotNull(family, "family is null");
    }

    @Override
    public CheckAndMutateBuilder qualifier(byte[] qualifier) {
      this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null. Consider using" +
          " an empty byte array, or just do not call this method if you want a null qualifier");
      return this;
    }

    @Override
    public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
      throw new NotImplementedException("timeRange not supported in ThriftTable");
    }

    @Override
    public CheckAndMutateBuilder ifNotExists() {
      this.op = CompareOperator.EQUAL;
      this.value = null;
      return this;
    }

    @Override
    public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
      this.op = Preconditions.checkNotNull(compareOp, "compareOp is null");
      this.value = Preconditions.checkNotNull(value, "value is null");
      return this;
    }

    private void preCheck() {
      Preconditions.checkNotNull(op, "condition is null. You need to specify the condition by" +
          " calling ifNotExists/ifEquals/ifMatches before executing the request");
    }

    @Override
    public boolean thenPut(Put put) throws IOException {
      preCheck();
      RowMutations rowMutations = new RowMutations(put.getRow());
      rowMutations.add(put);
      return checkAndMutate(row, family, qualifier, op, value, rowMutations);
    }

    @Override
    public boolean thenDelete(Delete delete) throws IOException {
      preCheck();
      RowMutations rowMutations = new RowMutations(delete.getRow());
      rowMutations.add(delete);
      return checkAndMutate(row, family, qualifier, op, value, rowMutations);
    }

    @Override
    public boolean thenMutate(RowMutations mutation) throws IOException {
      preCheck();
      return checkAndMutate(row, family, qualifier, op, value, mutation);
    }
  }


  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
      byte[] value, RowMutations mutation) throws IOException {
    try {
      ByteBuffer valueBuffer = value == null? null : ByteBuffer.wrap(value);
      return client.checkAndMutate(tableNameInBytes, ByteBuffer.wrap(row), ByteBuffer.wrap(family),
          ByteBuffer.wrap(qualifier), ThriftUtilities.compareOpFromHBase(op), valueBuffer,
          ThriftUtilities.rowMutationsFromHBase(mutation));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilderImpl(row, family);
  }

  @Override
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    throw new NotImplementedException("Implement later");
  }

  @Override
  public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) {
    throw new NotImplementedException("Implement later");
  }

  @Override
  public List<CheckAndMutateResult> checkAndMutate(List<CheckAndMutate> checkAndMutates) {
    throw new NotImplementedException("Implement later");
  }

  @Override
  public Result mutateRow(RowMutations rm) throws IOException {
    TRowMutations tRowMutations = ThriftUtilities.rowMutationsFromHBase(rm);
    try {
      client.mutateRow(tableNameInBytes, tRowMutations);
      return Result.EMPTY_RESULT;
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    TAppend tAppend = ThriftUtilities.appendFromHBase(append);
    try {
      TResult tResult = client.append(tableNameInBytes, tAppend);
      return ThriftUtilities.resultFromThrift(tResult);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    TIncrement tIncrement = ThriftUtilities.incrementFromHBase(increment);
    try {
      TResult tResult = client.increment(tableNameInBytes, tIncrement);
      return ThriftUtilities.resultFromThrift(tResult);
    }  catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    tTransport.close();
  }

  @Override
  public long getRpcTimeout(TimeUnit unit) {
    return unit.convert(operationTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return unit.convert(operationTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return unit.convert(operationTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return unit.convert(operationTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new NotImplementedException("coprocessorService not supported in ThriftTable");
  }

  @Override
  public RegionLocator getRegionLocator() throws IOException {
    throw new NotImplementedException("getRegionLocator not supported in ThriftTable");
  }
}

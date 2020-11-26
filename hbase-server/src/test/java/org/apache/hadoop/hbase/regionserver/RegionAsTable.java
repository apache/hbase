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
package org.apache.hadoop.hbase.regionserver;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
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
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

/**
 * An implementation of {@link Table} that sits directly on a Region; it decorates the passed in
 * Region instance with the Table API. Some API is not implemented yet (throws
 * {@link UnsupportedOperationException}) mostly because no need as yet or it necessitates copying
 * a load of code local from RegionServer.
 * 
 * <p>Use as an instance of a {@link Table} in-the-small -- no networking or servers
 * necessary -- or to write a test that can run directly against the datastore and then
 * over the network.
 */
public class RegionAsTable implements Table {
  private final Region region;

  /**
   * @param region Region to decorate with Table API.
   */
  public RegionAsTable(final Region region) {
    this.region = region;
  }

  @Override
  public TableName getName() {
    return this.region.getTableDescriptor().getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public HTableDescriptor getTableDescriptor() throws IOException {
    return new HTableDescriptor(this.region.getTableDescriptor());
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return this.region.getTableDescriptor();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    if (!get.isCheckExistenceOnly()) throw new IllegalArgumentException();
    return get(get) != null;
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    boolean [] results = new boolean[gets.size()];
    int index = 0;
    for (Get get: gets) {
      results[index++] = exists(get);
    }
    return results;
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
  throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Callback<R> callback)
  throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Result get(Get get) throws IOException {
    return this.region.get(get);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    Result [] results = new Result[gets.size()];
    int index = 0;
    for (Get get: gets) {
      results[index++] = get(get);
    }
    return results;
  }

  static class RegionScannerToResultScannerAdaptor implements ResultScanner {
    private static final Result [] EMPTY_RESULT_ARRAY = new Result[0];
    private final RegionScanner regionScanner;

    RegionScannerToResultScannerAdaptor(final RegionScanner regionScanner) {
      this.regionScanner = regionScanner;
    }

    @Override
    public Iterator<Result> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Result next() throws IOException {
      List<Cell> cells = new ArrayList<>();
      return regionScanner.next(cells)? Result.create(cells): null;
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
      List<Result> results = new ArrayList<>(nbRows);
      for (int i = 0; i < nbRows; i++) {
        Result result = next();
        if (result == null) break;
        results.add(result);
      }
      return results.toArray(EMPTY_RESULT_ARRAY);
    }

    @Override
    public void close() {
      try {
        regionScanner.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean renewLease() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScanMetrics getScanMetrics() {
      throw new UnsupportedOperationException();
    }
  };

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return new RegionScannerToResultScannerAdaptor(this.region.getScanner(scan));
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getScanner(new Scan().addFamily(family));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  @Override
  public void put(Put put) throws IOException {
    this.region.put(put);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    for (Put put: puts) put(put);
  }

  @Override
  @Deprecated
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, Put put)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                             CompareOperator compareOp, byte[] value, Put put)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(Delete delete) throws IOException {
    this.region.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    for(Delete delete: deletes) delete(delete);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Delete delete)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                                CompareOperator compareOp, byte[] value, Delete delete)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Result mutateRow(RowMutations rm) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Result append(Append append) throws IOException {
    return this.region.append(append);
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return this.region.increment(increment);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * This call will NOT close the underlying region.
   */
  @Override
  public void close() throws IOException {
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable)
  throws ServiceException, Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback)
  throws ServiceException, Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(MethodDescriptor
      methodDescriptor, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype)
  throws ServiceException, Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
  throws ServiceException, Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, RowMutations mutation)
  throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOperator compareOp, byte[] value, RowMutations mutation) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public void setOperationTimeout(int operationTimeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public int getOperationTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public void setRpcTimeout(int rpcTimeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public void setWriteRpcTimeout(int writeRpcTimeout) {throw new UnsupportedOperationException(); }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public void setReadRpcTimeout(int readRpcTimeout) {throw new UnsupportedOperationException(); }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public int getRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRpcTimeout(TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public int getWriteRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public int getReadRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RegionLocator getRegionLocator() throws IOException {
    throw new UnsupportedOperationException();
  }
}

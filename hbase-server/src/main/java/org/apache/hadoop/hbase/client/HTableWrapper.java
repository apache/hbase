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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost.Environment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.io.MultipleIOException;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * A wrapper for HTable. Can be used to restrict privilege.
 *
 * Currently it just helps to track tables opened by a Coprocessor and
 * facilitate close of them if it is aborted.
 *
 * We also disallow row locking.
 *
 * There is nothing now that will stop a coprocessor from using HTable
 * objects directly instead of this API, but in the future we intend to
 * analyze coprocessor implementations as they are loaded and reject those
 * which attempt to use objects and methods outside the Environment
 * sandbox.
 */
public class HTableWrapper implements HTableInterface {

  private TableName tableName;
  private final Table table;
  private ClusterConnection connection;
  private final List<HTableInterface> openTables;

  /**
   * @param openTables External list of tables used for tracking wrappers.
   * @throws IOException
   */
  public static HTableInterface createWrapper(List<HTableInterface> openTables,
      TableName tableName, Environment env, ExecutorService pool) throws IOException {
    return new HTableWrapper(openTables, tableName,
        CoprocessorHConnection.getConnectionForEnvironment(env), pool);
  }

  private HTableWrapper(List<HTableInterface> openTables, TableName tableName,
      ClusterConnection connection, ExecutorService pool)
      throws IOException {
    this.tableName = tableName;
    this.table = connection.getTable(tableName, pool);
    this.connection = connection;
    this.openTables = openTables;
    this.openTables.add(this);
  }

  public void internalClose() throws IOException {
    List<IOException> exceptions = new ArrayList<IOException>(2);
    try {
      table.close();
    } catch (IOException e) {
      exceptions.add(e);
    }
    try {
      // have to self-manage our connection, as per the HTable contract
      if (this.connection != null) {
        this.connection.close();
      }
    } catch (IOException e) {
      exceptions.add(e);
    }
    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }

  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  public void close() throws IOException {
    try {
      internalClose();
    } finally {
      openTables.remove(this);
    }
  }

  @Deprecated
  public Result getRowOrBefore(byte[] row, byte[] family)
      throws IOException {
    Scan scan = Scan.createGetClosestRowOrBeforeReverseScan(row);
    Result startRowResult = null;
    try (ResultScanner resultScanner = this.table.getScanner(scan)) {
      startRowResult = resultScanner.next();
    }
    return startRowResult;
  }

  public Result get(Get get) throws IOException {
    return table.get(get);
  }

  public boolean exists(Get get) throws IOException {
    return table.exists(get);
  }

  public boolean[] existsAll(List<Get> gets) throws IOException{
    return table.existsAll(gets);
  }

  @Deprecated
  public Boolean[] exists(List<Get> gets) throws IOException {
    // Do convertion.
    boolean [] exists = table.existsAll(gets);
    if (exists == null) return null;
    Boolean [] results = new Boolean [exists.length];
    for (int i = 0; i < exists.length; i++) {
      results[i] = exists[i]? Boolean.TRUE: Boolean.FALSE;
    }
    return results;
  }

  public void put(Put put) throws IOException {
    table.put(put);
  }

  public void put(List<Put> puts) throws IOException {
    table.put(puts);
  }

  public void delete(Delete delete) throws IOException {
    table.delete(delete);
  }

  public void delete(List<Delete> deletes) throws IOException {
    table.delete(deletes);
  }

  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    return table.checkAndPut(row, family, qualifier, value, put);
  }

  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Put put) throws IOException {
    return table.checkAndPut(row, family, qualifier, compareOp, value, put);
  }

  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    return table.checkAndDelete(row, family, qualifier, value, delete);
  }

  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Delete delete) throws IOException {
    return table.checkAndDelete(row, family, qualifier, compareOp, value, delete);
  }

  public long incrementColumnValue(byte[] row, byte[] family,
      byte[] qualifier, long amount) throws IOException {
    return table.incrementColumnValue(row, family, qualifier, amount);
  }

  public long incrementColumnValue(byte[] row, byte[] family,
      byte[] qualifier, long amount, Durability durability)
      throws IOException {
    return table.incrementColumnValue(row, family, qualifier, amount,
        durability);
  }

  @Override
  public Result append(Append append) throws IOException {
    return table.append(append);
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return table.increment(increment);
  }

  public void flushCommits() throws IOException {
    table.flushCommits();
  }

  public boolean isAutoFlush() {
    return table.isAutoFlush();
  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    return table.getScanner(scan);
  }

  public ResultScanner getScanner(byte[] family) throws IOException {
    return table.getScanner(family);
  }

  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    return table.getScanner(family, qualifier);
  }

  public HTableDescriptor getTableDescriptor() throws IOException {
    return table.getTableDescriptor();
  }

  @Override
  public byte[] getTableName() {
    return tableName.getName();
  }

  @Override
  public TableName getName() {
    return table.getName();
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    table.batch(actions, results);
  }

  /**
   * {@inheritDoc}
   * @deprecated If any exception is thrown by one of the actions, there is no way to
   * retrieve the partially executed results. Use {@link #batch(List, Object[])} instead.
   */
  @Override
  public Object[] batch(List<? extends Row> actions)
      throws IOException, InterruptedException {
    return table.batch(actions);
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    table.batchCallback(actions, results, callback);
  }

  /**
   * {@inheritDoc}
   * @deprecated If any exception is thrown by one of the actions, there is no way to
   * retrieve the partially executed results. Use
   * {@link #batchCallback(List, Object[], org.apache.hadoop.hbase.client.coprocessor.Batch.Callback)}
   * instead.
   */
  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    return table.batchCallback(actions, callback);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return table.get(gets);
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return table.coprocessorService(row);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Batch.Call<T, R> callable)
      throws ServiceException, Throwable {
    return table.coprocessorService(service, startKey, endKey, callable);
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    table.coprocessorService(service, startKey, endKey, callable, callback);
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    table.mutateRow(rm);
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    table.setAutoFlushTo(autoFlush);
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    setAutoFlush(autoFlush);
    if (!autoFlush && !clearBufferOnFail) {
      // We don't support his combination.  In HTable, the implementation is this:
      //
      // this.clearBufferOnFail = autoFlush || clearBufferOnFail
      //
      // So if autoFlush == false and clearBufferOnFail is false, that is not supported in
      // the new Table Interface so just throwing UnsupportedOperationException here.
      throw new UnsupportedOperationException("Can't do this via wrapper");
    }
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    table.setAutoFlushTo(autoFlush);
  }

  @Override
  public long getWriteBufferSize() {
     return table.getWriteBufferSize();
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    table.setWriteBufferSize(writeBufferSize);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family,
      byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
    return table.incrementColumnValue(row, family, qualifier, amount,
        writeToWAL? Durability.USE_DEFAULT: Durability.SKIP_WAL);
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey,
      R responsePrototype) throws ServiceException, Throwable {
    return table.batchCoprocessorService(methodDescriptor, request, startKey, endKey,
      responsePrototype);
  }

  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype,
      callback);
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, RowMutations rm) throws IOException {
    return table.checkAndMutate(row, family, qualifier, compareOp, value, rm);
  }
}

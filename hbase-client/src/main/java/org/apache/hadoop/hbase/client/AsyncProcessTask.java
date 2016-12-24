/*
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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

/**
 * Contains the attributes of a task which will be executed
 * by {@link org.apache.hadoop.hbase.client.AsyncProcess}.
 * The attributes will be validated by AsyncProcess.
 * It's intended for advanced client applications.
 * @param <T> The type of response from server-side
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncProcessTask<T> {
  /**
   * The number of processed rows.
   * The AsyncProcess has traffic control which may reject some rows.
   */
  public enum SubmittedRows {
    ALL,
    AT_LEAST_ONE,
    NORMAL
  }
  public static <T> Builder<T> newBuilder(final Batch.Callback<T> callback) {
    return new Builder<>(callback);
  }
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder<T> {

    private ExecutorService pool;
    private TableName tableName;
    private RowAccess<? extends Row> rows;
    private SubmittedRows submittedRows = SubmittedRows.ALL;
    private Batch.Callback<T> callback;
    private boolean needResults;
    private int rpcTimeout;
    private int operationTimeout;
    private CancellableRegionServerCallable callable;
    private Object[] results;

    private Builder() {
    }

    private Builder(Batch.Callback<T> callback) {
      this.callback = callback;
    }

    Builder<T> setResults(Object[] results) {
      this.results = results;
      if (results != null && results.length != 0) {
        setNeedResults(true);
      }
      return this;
    }

    public Builder<T> setPool(ExecutorService pool) {
      this.pool = pool;
      return this;
    }

    public Builder<T> setRpcTimeout(int rpcTimeout) {
      this.rpcTimeout = rpcTimeout;
      return this;
    }

    public Builder<T> setOperationTimeout(int operationTimeout) {
      this.operationTimeout = operationTimeout;
      return this;
    }

    public Builder<T> setTableName(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder<T> setRowAccess(List<? extends Row> rows) {
      this.rows = new ListRowAccess<>(rows);
      return this;
    }

    public Builder<T> setRowAccess(RowAccess<? extends Row> rows) {
      this.rows = rows;
      return this;
    }

    public Builder<T> setSubmittedRows(SubmittedRows submittedRows) {
      this.submittedRows = submittedRows;
      return this;
    }

    public Builder<T> setNeedResults(boolean needResults) {
      this.needResults = needResults;
      return this;
    }

    Builder<T> setCallable(CancellableRegionServerCallable callable) {
      this.callable = callable;
      return this;
    }

    public AsyncProcessTask<T> build() {
      return new AsyncProcessTask<>(pool, tableName, rows, submittedRows,
              callback, callable, needResults, rpcTimeout, operationTimeout, results);
    }
  }
  private final ExecutorService pool;
  private final TableName tableName;
  private final RowAccess<? extends Row> rows;
  private final SubmittedRows submittedRows;
  private final Batch.Callback<T> callback;
  private final CancellableRegionServerCallable callable;
  private final boolean needResults;
  private final int rpcTimeout;
  private final int operationTimeout;
  private final Object[] results;
  AsyncProcessTask(AsyncProcessTask<T> task) {
    this(task.getPool(), task.getTableName(), task.getRowAccess(),
        task.getSubmittedRows(), task.getCallback(), task.getCallable(),
        task.getNeedResults(), task.getRpcTimeout(), task.getOperationTimeout(),
        task.getResults());
  }
  AsyncProcessTask(ExecutorService pool, TableName tableName,
          RowAccess<? extends Row> rows, SubmittedRows size, Batch.Callback<T> callback,
          CancellableRegionServerCallable callable, boolean needResults,
          int rpcTimeout, int operationTimeout, Object[] results) {
    this.pool = pool;
    this.tableName = tableName;
    this.rows = rows;
    this.submittedRows = size;
    this.callback = callback;
    this.callable = callable;
    this.needResults = needResults;
    this.rpcTimeout = rpcTimeout;
    this.operationTimeout = operationTimeout;
    this.results = results;
  }

  public int getOperationTimeout() {
    return operationTimeout;
  }

  public ExecutorService getPool() {
    return pool;
  }

  public TableName getTableName() {
    return tableName;
  }

  public RowAccess<? extends Row> getRowAccess() {
    return rows;
  }

  public SubmittedRows getSubmittedRows() {
    return submittedRows;
  }

  public Batch.Callback<T> getCallback() {
    return callback;
  }

  CancellableRegionServerCallable getCallable() {
    return callable;
  }

  Object[] getResults() {
    return results;
  }

  public boolean getNeedResults() {
    return needResults;
  }

  public int getRpcTimeout() {
    return rpcTimeout;
  }

  static class ListRowAccess<T> implements RowAccess<T> {

    private final List<T> data;

    ListRowAccess(final List<T> data) {
      this.data = data;
    }

    @Override
    public int size() {
      return data.size();
    }

    @Override
    public boolean isEmpty() {
      return data.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
      return data.iterator();
    }
  }
}

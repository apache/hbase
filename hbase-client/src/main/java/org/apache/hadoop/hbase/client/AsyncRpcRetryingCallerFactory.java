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
package org.apache.hadoop.hbase.client;

import com.google.common.base.Preconditions;

import io.netty.util.HashedWheelTimer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Factory to create an AsyncRpcRetryCaller.
 */
@InterfaceAudience.Private
class AsyncRpcRetryingCallerFactory {

  private final AsyncConnectionImpl conn;

  private final HashedWheelTimer retryTimer;

  public AsyncRpcRetryingCallerFactory(AsyncConnectionImpl conn, HashedWheelTimer retryTimer) {
    this.conn = conn;
    this.retryTimer = retryTimer;
  }

  public class SingleRequestCallerBuilder<T> {

    private TableName tableName;

    private byte[] row;

    private AsyncSingleRequestRpcRetryingCaller.Callable<T> callable;

    private long operationTimeoutNs = -1L;

    private long rpcTimeoutNs = -1L;

    public SingleRequestCallerBuilder<T> table(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public SingleRequestCallerBuilder<T> row(byte[] row) {
      this.row = row;
      return this;
    }

    public SingleRequestCallerBuilder<T> action(
        AsyncSingleRequestRpcRetryingCaller.Callable<T> callable) {
      this.callable = callable;
      return this;
    }

    public SingleRequestCallerBuilder<T> operationTimeout(long operationTimeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(operationTimeout);
      return this;
    }

    public SingleRequestCallerBuilder<T> rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public AsyncSingleRequestRpcRetryingCaller<T> build() {
      return new AsyncSingleRequestRpcRetryingCaller<>(retryTimer, conn,
          Preconditions.checkNotNull(tableName, "tableName is null"),
          Preconditions.checkNotNull(row, "row is null"),
          Preconditions.checkNotNull(callable, "action is null"), conn.connConf.getPauseNs(),
          conn.connConf.getMaxRetries(), operationTimeoutNs, rpcTimeoutNs,
          conn.connConf.getStartLogErrorsCnt());
    }

    /**
     * Shortcut for {@code build().call()}
     */
    public CompletableFuture<T> call() {
      return build().call();
    }
  }

  /**
   * Create retry caller for single action, such as get, put, delete, etc.
   */
  public <T> SingleRequestCallerBuilder<T> single() {
    return new SingleRequestCallerBuilder<>();
  }
}

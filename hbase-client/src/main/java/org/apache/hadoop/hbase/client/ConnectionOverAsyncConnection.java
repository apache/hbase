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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.util.ConcurrentMapUtils.IOExceptionSupplier;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The connection implementation based on {@link AsyncConnection}.
 */
@InterfaceAudience.Private
class ConnectionOverAsyncConnection implements Connection {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionOverAsyncConnection.class);

  private volatile boolean aborted = false;

  // only used for executing coprocessor calls, as users may reference the methods in the
  // BlockingInterface of the protobuf stub so we have to execute the call in a separated thread...
  // Will be removed in 4.0.0 along with the deprecated coprocessor methods in Table and Admin
  // interface.
  private volatile ExecutorService batchPool = null;

  private final AsyncConnectionImpl conn;

  private final ConnectionConfiguration connConf;

  ConnectionOverAsyncConnection(AsyncConnectionImpl conn) {
    this.conn = conn;
    this.connConf = new ConnectionConfiguration(conn.getConfiguration());
  }

  @Override
  public void abort(String why, Throwable error) {
    if (error != null) {
      LOG.error(HBaseMarkers.FATAL, why, error);
    } else {
      LOG.error(HBaseMarkers.FATAL, why);
    }
    aborted = true;
    try {
      Closeables.close(this, true);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public Configuration getConfiguration() {
    return conn.getConfiguration();
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    AsyncBufferedMutatorBuilder builder = conn.getBufferedMutatorBuilder(params.getTableName());
    if (params.getRpcTimeout() != BufferedMutatorParams.UNSET) {
      builder.setRpcTimeout(params.getRpcTimeout(), TimeUnit.MILLISECONDS);
    }
    if (params.getOperationTimeout() != BufferedMutatorParams.UNSET) {
      builder.setOperationTimeout(params.getOperationTimeout(), TimeUnit.MILLISECONDS);
    }
    if (params.getWriteBufferSize() != BufferedMutatorParams.UNSET) {
      builder.setWriteBufferSize(params.getWriteBufferSize());
    }
    if (params.getWriteBufferPeriodicFlushTimeoutMs() != BufferedMutatorParams.UNSET) {
      builder.setWriteBufferPeriodicFlush(params.getWriteBufferPeriodicFlushTimeoutMs(),
        TimeUnit.MILLISECONDS);
    }
    if (params.getMaxKeyValueSize() != BufferedMutatorParams.UNSET) {
      builder.setMaxKeyValueSize(params.getMaxKeyValueSize());
    }
    return new BufferedMutatorOverAsyncBufferedMutator(builder.build(), params.getListener());
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return new RegionLocatorOverAsyncTableRegionLocator(conn.getRegionLocator(tableName));
  }

  @Override
  public void clearRegionLocationCache() {
    conn.clearRegionLocationCache();
  }

  @Override
  public Admin getAdmin() throws IOException {
    return new AdminOverAsyncAdmin(this, (RawAsyncHBaseAdmin) conn.getAdmin());
  }

  @Override
  public void close() throws IOException {
    conn.close();
  }

  // will be called from AsyncConnection, to avoid infinite loop as in the above method we will call
  // AsyncConnection.close.
  synchronized void closePool() {
    ExecutorService batchPool = this.batchPool;
    if (batchPool != null) {
      ConnectionUtils.shutdownPool(batchPool);
      this.batchPool = null;
    }
  }

  @Override
  public boolean isClosed() {
    return conn.isClosed();
  }

  // only used for executing coprocessor calls, as users may reference the methods in the
  // BlockingInterface of the protobuf stub so we have to execute the call in a separated thread...
  // Will be removed in 4.0.0 along with the deprecated coprocessor methods in Table and Admin
  // interface.
  private ThreadPoolExecutor createThreadPool() {
    Configuration conf = conn.getConfiguration();
    int threads = conf.getInt("hbase.hconnection.threads.max", 256);
    long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
    BlockingQueue<Runnable> workQueue =
      new LinkedBlockingQueue<>(threads * conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
        HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(threads, threads, keepAliveTime,
      TimeUnit.SECONDS, workQueue,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(toString() + "-shared-%d").build());
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  // only used for executing coprocessor calls, as users may reference the methods in the
  // BlockingInterface of the protobuf stub so we have to execute the call in a separated thread...
  // Will be removed in 4.0.0 along with the deprecated coprocessor methods in Table and Admin
  // interface.
  private ExecutorService getBatchPool() throws IOException {
    if (batchPool == null) {
      synchronized (this) {
        if (isClosed()) {
          throw new DoNotRetryIOException("Connection is closed");
        }
        if (batchPool == null) {
          this.batchPool = createThreadPool();
        }
      }
    }
    return this.batchPool;
  }

  @Override
  public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
    return new TableBuilderBase(tableName, connConf) {

      @Override
      public Table build() {
        IOExceptionSupplier<ExecutorService> poolSupplier =
          pool != null ? () -> pool : ConnectionOverAsyncConnection.this::getBatchPool;
        return new TableOverAsyncTable(conn,
          conn.getTableBuilder(tableName).setRpcTimeout(rpcTimeout, TimeUnit.MILLISECONDS)
            .setReadRpcTimeout(readRpcTimeout, TimeUnit.MILLISECONDS)
            .setWriteRpcTimeout(writeRpcTimeout, TimeUnit.MILLISECONDS)
            .setOperationTimeout(operationTimeout, TimeUnit.MILLISECONDS)
            .build(),
          poolSupplier);
      }
    };
  }

  @Override
  public AsyncConnection toAsyncConnection() {
    return conn;
  }

  @Override
  public String getClusterId() {
    return conn.getClusterId();
  }

  @Override
  public Hbck getHbck() throws IOException {
    return FutureUtils.get(conn.getHbck());
  }

  @Override
  public Hbck getHbck(ServerName masterServer) throws IOException {
    return conn.getHbck(masterServer);
  }

  /**
   * An identifier that will remain the same for a given connection.
   */
  @Override
  public String toString() {
    return "connection-over-async-connection-0x" + Integer.toHexString(hashCode());
  }
}

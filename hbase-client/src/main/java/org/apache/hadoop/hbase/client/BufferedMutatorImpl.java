/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Used to communicate with a single HBase table similar to {@link HTable}
 * but meant for batched, potentially asynchronous puts. Obtain an instance from
 * a {@link Connection} and call {@link #close()} afterwards.
 * </p>
 *
 * @see ConnectionFactory
 * @see Connection
 * @since 1.0.0
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BufferedMutatorImpl implements BufferedMutator {

  private static final Log LOG = LogFactory.getLog(BufferedMutatorImpl.class);
  
  private final ExceptionListener listener;

  protected ClusterConnection connection; // non-final so can be overridden in test
  private final TableName tableName;
  private volatile Configuration conf;
  private List<Row> writeAsyncBuffer = new LinkedList<>();
  private long writeBufferSize;
  private final int maxKeyValueSize;
  protected long currentWriteBufferSize = 0;
  private boolean closed = false;
  private final ExecutorService pool;
  protected AsyncProcess ap; // non-final so can be overridden in test

  BufferedMutatorImpl(ClusterConnection conn, RpcRetryingCallerFactory rpcCallerFactory,
      RpcControllerFactory rpcFactory, BufferedMutatorParams params) {
    if (conn == null || conn.isClosed()) {
      throw new IllegalArgumentException("Connection is null or closed.");
    }

    this.tableName = params.getTableName();
    this.connection = conn;
    this.conf = connection.getConfiguration();
    this.pool = params.getPool();
    this.listener = params.getListener();

    ConnectionConfiguration tableConf = new ConnectionConfiguration(conf);
    this.writeBufferSize = params.getWriteBufferSize() != BufferedMutatorParams.UNSET ?
        params.getWriteBufferSize() : tableConf.getWriteBufferSize();
    this.maxKeyValueSize = params.getMaxKeyValueSize() != BufferedMutatorParams.UNSET ?
        params.getMaxKeyValueSize() : tableConf.getMaxKeyValueSize();

    // puts need to track errors globally due to how the APIs currently work.
    ap = new AsyncProcess(connection, conf, pool, rpcCallerFactory, true, rpcFactory);
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
  public synchronized void mutate(Mutation m) throws InterruptedIOException,
      RetriesExhaustedWithDetailsException {
    doMutate(m);
  }

  @Override
  public synchronized void mutate(List<? extends Mutation> ms) throws InterruptedIOException,
      RetriesExhaustedWithDetailsException {
    for (Mutation m : ms) {
      doMutate(m);
    }
  }

  /**
   * Add the put to the buffer. If the buffer is already too large, sends the buffer to the
   * cluster.
   *
   * @throws RetriesExhaustedWithDetailsException if there is an error on the cluster.
   * @throws InterruptedIOException if we were interrupted.
   */
  private void doMutate(Mutation m) throws InterruptedIOException,
      RetriesExhaustedWithDetailsException {
    if (closed) {
      throw new IllegalStateException("Cannot put when the BufferedMutator is closed.");
    }
    if (!(m instanceof Put) && !(m instanceof Delete)) {
      throw new IllegalArgumentException("Pass a Delete or a Put");
    }

    // This behavior is highly non-intuitive... it does not protect us against
    // 94-incompatible behavior, which is a timing issue because hasError, the below code
    // and setter of hasError are not synchronized. Perhaps it should be removed.
    if (ap.hasError()) {
      writeAsyncBuffer.add(m);
      backgroundFlushCommits(true);
    }

    if (m instanceof Put) {
      validatePut((Put) m);
    }

    currentWriteBufferSize += m.heapSize();
    writeAsyncBuffer.add(m);

    while (currentWriteBufferSize > writeBufferSize) {
      backgroundFlushCommits(false);
    }
  }

  // validate for well-formedness
  public void validatePut(final Put put) throws IllegalArgumentException {
    HTable.validatePut(put, maxKeyValueSize);
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.closed) {
      return;
    }
    try {
      // As we can have an operation in progress even if the buffer is empty, we call
      // backgroundFlushCommits at least one time.
      backgroundFlushCommits(true);
      this.pool.shutdown();
      boolean terminated = false;
      int loopCnt = 0;
      do {
        // wait until the pool has terminated
        terminated = this.pool.awaitTermination(60, TimeUnit.SECONDS);
        loopCnt += 1;
        if (loopCnt >= 10) {
          LOG.warn("close() failed to terminate pool after 10 minutes. Abandoning pool.");
          break;
        }
      } while (!terminated);
    } catch (InterruptedException e) {
      LOG.warn("waitForTermination interrupted");
    } finally {
      this.closed = true;
    }
  }

  @Override
  public synchronized void flush() throws InterruptedIOException,
      RetriesExhaustedWithDetailsException {
    // As we can have an operation in progress even if the buffer is empty, we call
    // backgroundFlushCommits at least one time.
    backgroundFlushCommits(true);
  }

  /**
   * Send the operations in the buffer to the servers. Does not wait for the server's answer. If
   * the is an error (max retried reach from a previous flush or bad operation), it tries to send
   * all operations in the buffer and sends an exception.
   *
   * @param synchronous - if true, sends all the writes and wait for all of them to finish before
   *        returning.
   */
  private void backgroundFlushCommits(boolean synchronous) throws InterruptedIOException,
      RetriesExhaustedWithDetailsException {
    try {
      if (!synchronous) {
        ap.submit(tableName, writeAsyncBuffer, true, null, false);
        if (ap.hasError()) {
          LOG.debug(tableName + ": One or more of the operations have failed -"
              + " waiting for all operation in progress to finish (successfully or not)");
        }
      }
      if (synchronous || ap.hasError()) {
        while (!writeAsyncBuffer.isEmpty()) {
          ap.submit(tableName, writeAsyncBuffer, true, null, false);
        }
        RetriesExhaustedWithDetailsException error = ap.waitForAllPreviousOpsAndReset(null);
        if (error != null) {
          if (listener == null) {
            throw error;
          } else {
            this.listener.onException(error, this);
          }
        }
      }
    } finally {
      currentWriteBufferSize = 0;
      for (Row mut : writeAsyncBuffer) {
        if (mut instanceof Mutation) {
          currentWriteBufferSize += ((Mutation) mut).heapSize();
        }
      }
    }
  }

  /**
   * This is used for legacy purposes in {@link HTable#setWriteBufferSize(long)} only. This ought
   * not be called for production uses.
   * @deprecated Going away when we drop public support for {@link HTableInterface}.
   */
  @Deprecated
  public void setWriteBufferSize(long writeBufferSize) throws RetriesExhaustedWithDetailsException,
      InterruptedIOException {
    this.writeBufferSize = writeBufferSize;
    if (currentWriteBufferSize > writeBufferSize) {
      flush();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getWriteBufferSize() {
    return this.writeBufferSize;
  }

  /**
   * This is used for legacy purposes in {@link HTable#getWriteBuffer()} only. This should not beÓ
   * called from production uses.
   * @deprecated Going away when we drop public support for {@link HTableInterface}.
Ó   */
  @Deprecated
  public List<Row> getWriteBuffer() {
    return this.writeAsyncBuffer;
  }
}

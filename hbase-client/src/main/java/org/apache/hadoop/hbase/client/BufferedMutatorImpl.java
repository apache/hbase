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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants; // Needed for write rpc timeout
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * Used to communicate with a single HBase table similar to {@link Table}
 * but meant for batched, potentially asynchronous puts. Obtain an instance from
 * a {@link Connection} and call {@link #close()} afterwards.
 * </p>
 *
 * <p>
 * While this can be used accross threads, great care should be used when doing so.
 * Errors are global to the buffered mutator and the Exceptions can be thrown on any
 * thread that causes the flush for requests.
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

  @VisibleForTesting
  final ConcurrentLinkedQueue<Mutation> writeAsyncBuffer = new ConcurrentLinkedQueue<Mutation>();
  @VisibleForTesting
  AtomicLong currentWriteBufferSize = new AtomicLong(0);

  /**
   * Count the size of {@link BufferedMutatorImpl#writeAsyncBuffer}.
   * The {@link ConcurrentLinkedQueue#size()} is NOT a constant-time operation.
   */
  @VisibleForTesting
  AtomicInteger undealtMutationCount = new AtomicInteger(0);
  private long writeBufferSize;
  private final int maxKeyValueSize;
  private boolean closed = false;
  private final ExecutorService pool;
  private int writeRpcTimeout; // needed to pass in through AsyncProcess constructor
  private int operationTimeout;

  @VisibleForTesting
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

    this.writeRpcTimeout = conn.getConfiguration().getInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY,
        conn.getConfiguration().getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
            HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    this.operationTimeout = conn.getConfiguration().getInt(
        HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    // puts need to track errors globally due to how the APIs currently work.
    ap = new AsyncProcess(connection, conf, pool, rpcCallerFactory, true, rpcFactory,
        writeRpcTimeout, operationTimeout);
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
  public void mutate(Mutation m) throws InterruptedIOException,
      RetriesExhaustedWithDetailsException {
    mutate(Arrays.asList(m));
  }

  @Override
  public void mutate(List<? extends Mutation> ms) throws InterruptedIOException,
      RetriesExhaustedWithDetailsException {

    if (closed) {
      throw new IllegalStateException("Cannot put when the BufferedMutator is closed.");
    }

    long toAddSize = 0;
    int toAddCount = 0;
    for (Mutation m : ms) {
      if (m instanceof Put) {
        validatePut((Put) m);
      }
      toAddSize += m.heapSize();
      ++toAddCount;
    }

    // This behavior is highly non-intuitive... it does not protect us against
    // 94-incompatible behavior, which is a timing issue because hasError, the below code
    // and setter of hasError are not synchronized. Perhaps it should be removed.
    if (ap.hasError()) {
      currentWriteBufferSize.addAndGet(toAddSize);
      writeAsyncBuffer.addAll(ms);
      undealtMutationCount.addAndGet(toAddCount);
      backgroundFlushCommits(true);
    } else {
      currentWriteBufferSize.addAndGet(toAddSize);
      writeAsyncBuffer.addAll(ms);
      undealtMutationCount.addAndGet(toAddCount);
    }

    // Now try and queue what needs to be queued.
    while (undealtMutationCount.get() != 0
        && currentWriteBufferSize.get() > writeBufferSize) {
      backgroundFlushCommits(false);
    }
  }

  // validate for well-formedness
  public void validatePut(final Put put) throws IllegalArgumentException {
    HTable.validatePut(put, maxKeyValueSize);
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (this.closed) {
        return;
      }
      // As we can have an operation in progress even if the buffer is empty, we call
      // backgroundFlushCommits at least one time.
      backgroundFlushCommits(true);
      this.pool.shutdown();
      boolean terminated;
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
  private void backgroundFlushCommits(boolean synchronous) throws
      InterruptedIOException,
      RetriesExhaustedWithDetailsException {
    if (!synchronous && writeAsyncBuffer.isEmpty()) {
      return;
    }

    if (!synchronous) {
      QueueRowAccess taker = new QueueRowAccess();
      try {
        ap.submit(tableName, taker, true, null, false);
        if (ap.hasError()) {
          LOG.debug(tableName + ": One or more of the operations have failed -"
              + " waiting for all operation in progress to finish (successfully or not)");
        }
      } finally {
        taker.restoreRemainder();
      }
    }
    if (synchronous || ap.hasError()) {
      QueueRowAccess taker = new QueueRowAccess();
      try {
        while (!taker.isEmpty()) {
          ap.submit(tableName, taker, true, null, false);
          taker.reset();
        }
      } finally {
        taker.restoreRemainder();
      }

      RetriesExhaustedWithDetailsException error =
          ap.waitForAllPreviousOpsAndReset(null, tableName.getNameAsString());
      if (error != null) {
        if (listener == null) {
          throw error;
        } else {
          this.listener.onException(error, this);
        }
      }
    }
  }

  /**
   * This is used for legacy purposes in {@link HTable#setWriteBufferSize(long)} only. This ought
   * not be called for production uses.
   * @deprecated Going away when we drop public support for {@link HTable}.
   */
  @Deprecated
  public void setWriteBufferSize(long writeBufferSize) throws RetriesExhaustedWithDetailsException,
      InterruptedIOException {
    this.writeBufferSize = writeBufferSize;
    if (currentWriteBufferSize.get() > writeBufferSize) {
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

  @Override
  public void setRpcTimeout(int timeout) {
    this.writeRpcTimeout = timeout;
    ap.setRpcTimeout(timeout);
  }

  @Override
  public void setOperationTimeout(int timeout) {
    this.operationTimeout = timeout;
    ap.setOperationTimeout(operationTimeout);
  }

  private class QueueRowAccess implements RowAccess<Row> {
    private int remainder = undealtMutationCount.getAndSet(0);

    void reset() {
      restoreRemainder();
      remainder = undealtMutationCount.getAndSet(0);
    }

    @Override
    public Iterator<Row> iterator() {
      return new Iterator<Row>() {
        private final Iterator<Mutation> iter = writeAsyncBuffer.iterator();
        private int countDown = remainder;
        private Mutation last = null;
        @Override
        public boolean hasNext() {
          if (countDown <= 0) {
            return false;
          }
          return iter.hasNext();
        }
        @Override
        public Row next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          last = iter.next();
          if (last == null) {
            throw new NoSuchElementException();
          }
          --countDown;
          return last;
        }
        @Override
        public void remove() {
          if (last == null) {
            throw new IllegalStateException();
          }
          iter.remove();
          currentWriteBufferSize.addAndGet(-last.heapSize());
          --remainder;
        }
      };
    }

    @Override
    public int size() {
      return remainder;
    }

    void restoreRemainder() {
      if (remainder > 0) {
        undealtMutationCount.addAndGet(remainder);
        remainder = 0;
      }
    }

    @Override
    public boolean isEmpty() {
      return remainder <= 0;
    }
  }
}

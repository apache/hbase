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

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

/**
 * <p>
 * Used to communicate with a single HBase table similar to {@link Table}
 * but meant for batched, potentially asynchronous puts. Obtain an instance from
 * a {@link Connection} and call {@link #close()} afterwards. Provide an alternate
 * to this implementation by setting {@link BufferedMutatorParams#implementationClassName(String)}
 * or by setting alternate classname via the key {} in Configuration.
 * </p>
 *
 * <p>
 * While this can be used across threads, great care should be used when doing so.
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

  private final TableName tableName;

  private final Configuration conf;
  private final ConcurrentLinkedQueue<Mutation> writeAsyncBuffer = new ConcurrentLinkedQueue<>();
  private final AtomicLong currentWriteBufferSize = new AtomicLong(0);
  /**
   * Count the size of {@link BufferedMutatorImpl#writeAsyncBuffer}.
   * The {@link ConcurrentLinkedQueue#size()} is NOT a constant-time operation.
   */
  private final AtomicInteger undealtMutationCount = new AtomicInteger(0);
  private volatile long writeBufferSize;
  private final int maxKeyValueSize;
  private final ExecutorService pool;
  private final AtomicInteger rpcTimeout;
  private final AtomicInteger operationTimeout;
  private final boolean cleanupPoolOnClose;
  private volatile boolean closed = false;
  private final AsyncProcess ap;

  @VisibleForTesting
  BufferedMutatorImpl(ClusterConnection conn, BufferedMutatorParams params, AsyncProcess ap) {
    if (conn == null || conn.isClosed()) {
      throw new IllegalArgumentException("Connection is null or closed.");
    }
    this.tableName = params.getTableName();
    this.conf = conn.getConfiguration();
    this.listener = params.getListener();
    if (params.getPool() == null) {
      this.pool = HTable.getDefaultExecutor(conf);
      cleanupPoolOnClose = true;
    } else {
      this.pool = params.getPool();
      cleanupPoolOnClose = false;
    }
    ConnectionConfiguration tableConf = new ConnectionConfiguration(conf);
    this.writeBufferSize = params.getWriteBufferSize() != BufferedMutatorParams.UNSET ?
        params.getWriteBufferSize() : tableConf.getWriteBufferSize();
    this.maxKeyValueSize = params.getMaxKeyValueSize() != BufferedMutatorParams.UNSET ?
        params.getMaxKeyValueSize() : tableConf.getMaxKeyValueSize();

    this.rpcTimeout = new AtomicInteger(params.getRpcTimeout() != BufferedMutatorParams.UNSET ?
    params.getRpcTimeout() : conn.getConnectionConfiguration().getWriteRpcTimeout());
    this.operationTimeout = new AtomicInteger(params.getOperationTimeout()!= BufferedMutatorParams.UNSET ?
    params.getOperationTimeout() : conn.getConnectionConfiguration().getOperationTimeout());
    this.ap = ap;
  }
  BufferedMutatorImpl(ClusterConnection conn, RpcRetryingCallerFactory rpcCallerFactory,
      RpcControllerFactory rpcFactory, BufferedMutatorParams params) {
    this(conn, params,
      // puts need to track errors globally due to how the APIs currently work.
      new AsyncProcess(conn, conn.getConfiguration(), rpcCallerFactory, true, rpcFactory));
  }

  @VisibleForTesting
  ExecutorService getPool() {
    return pool;
  }

  @VisibleForTesting
  AsyncProcess getAsyncProcess() {
    return ap;
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
    mutate(Collections.singletonList(m));
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
      if (cleanupPoolOnClose) {
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
      }
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
      AsyncProcessTask task = wrapAsyncProcessTask(taker);
      try {
        ap.submit(task);
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
      AsyncProcessTask task = wrapAsyncProcessTask(taker);
      try {
        while (!taker.isEmpty()) {
          ap.submit(task);
          taker.reset();
        }
      } finally {
        taker.restoreRemainder();
      }
      RetriesExhaustedWithDetailsException error =
          ap.waitForAllPreviousOpsAndReset(null, tableName);
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
   * Reuse the AsyncProcessTask when calling {@link BufferedMutatorImpl#backgroundFlushCommits(boolean)}.
   * @param taker access the inner buffer.
   * @return An AsyncProcessTask which always returns the latest rpc and operation timeout.
   */
  private AsyncProcessTask wrapAsyncProcessTask(QueueRowAccess taker) {
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
        .setPool(pool)
        .setTableName(tableName)
        .setRowAccess(taker)
        .setSubmittedRows(AsyncProcessTask.SubmittedRows.AT_LEAST_ONE)
        .build();
    return new AsyncProcessTask(task) {
      @Override
      public int getRpcTimeout() {
        return rpcTimeout.get();
      }

      @Override
      public int getOperationTimeout() {
        return operationTimeout.get();
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getWriteBufferSize() {
    return this.writeBufferSize;
  }

  @Override
  public void setRpcTimeout(int rpcTimeout) {
    this.rpcTimeout.set(rpcTimeout);
  }

  @Override
  public void setOperationTimeout(int operationTimeout) {
    this.operationTimeout.set(operationTimeout);
  }

  @VisibleForTesting
  long getCurrentWriteBufferSize() {
    return currentWriteBufferSize.get();
  }

  @VisibleForTesting
  int size() {
    return undealtMutationCount.get();
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

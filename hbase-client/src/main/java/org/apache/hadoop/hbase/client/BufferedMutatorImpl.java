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

import static org.apache.hadoop.hbase.client.BufferedMutatorParams.UNSET;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

/**
 * <p>
 * Used to communicate with a single HBase table similar to {@link HTable}
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
  final AtomicInteger undealtMutationCount = new AtomicInteger(0);
  private long writeBufferSize;
  /**
   * Having the timer tick run more often that once every 100ms is needless and will
   * probably cause too many timer events firing having a negative impact on performance.
   */
  public static final long MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS = 100;

  private final AtomicLong writeBufferPeriodicFlushTimeoutMs = new AtomicLong(0);
  private final AtomicLong writeBufferPeriodicFlushTimerTickMs =
          new AtomicLong(MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS);
  private Timer writeBufferPeriodicFlushTimer = null;

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

    ConnectionConfiguration connConf = conn.getConnectionConfiguration();
    if (connConf == null) {
      // Slow: parse conf in ConnectionConfiguration constructor
      connConf = new ConnectionConfiguration(conf);
    }
    this.writeBufferSize = params.getWriteBufferSize() != BufferedMutatorParams.UNSET ?
        params.getWriteBufferSize() : connConf.getWriteBufferSize();

    // Set via the setter because it does value validation and starts/stops the TimerTask
    long newWriteBufferPeriodicFlushTimeoutMs =
            params.getWriteBufferPeriodicFlushTimeoutMs() != UNSET
                    ? params.getWriteBufferPeriodicFlushTimeoutMs()
                    : connConf.getWriteBufferPeriodicFlushTimeoutMs();
    long newWriteBufferPeriodicFlushTimerTickMs =
            params.getWriteBufferPeriodicFlushTimerTickMs() != UNSET
                    ? params.getWriteBufferPeriodicFlushTimerTickMs()
                    : connConf.getWriteBufferPeriodicFlushTimerTickMs();
    this.setWriteBufferPeriodicFlush(
            newWriteBufferPeriodicFlushTimeoutMs,
            newWriteBufferPeriodicFlushTimerTickMs);

    this.maxKeyValueSize = params.getMaxKeyValueSize() != BufferedMutatorParams.UNSET ?
        params.getMaxKeyValueSize() : connConf.getMaxKeyValueSize();

    this.writeRpcTimeout = connConf.getWriteRpcTimeout();
    this.operationTimeout = connConf.getOperationTimeout();
    // puts need to track errors globally due to how the APIs currently work.
    ap = new AsyncProcess(connection, conf, pool, rpcCallerFactory, true, rpcFactory, writeRpcTimeout);
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

    if (currentWriteBufferSize.get() == 0) {
      firstRecordInBufferTimestamp.set(System.currentTimeMillis());
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

  @VisibleForTesting
  protected long getExecutedWriteBufferPeriodicFlushes() {
    return executedWriteBufferPeriodicFlushes.get();
  }

  private final AtomicLong firstRecordInBufferTimestamp = new AtomicLong(0);
  private final AtomicLong executedWriteBufferPeriodicFlushes = new AtomicLong(0);

  private void timerCallbackForWriteBufferPeriodicFlush() {
    if (currentWriteBufferSize.get() == 0) {
      return; // Nothing to flush
    }
    long now = System.currentTimeMillis();
    if (firstRecordInBufferTimestamp.get() + writeBufferPeriodicFlushTimeoutMs.get() > now) {
      return; // No need to flush yet
    }
    // The first record in the writebuffer has been in there too long --> flush
    try {
      executedWriteBufferPeriodicFlushes.incrementAndGet();
      flush();
    } catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
      LOG.error("Exception during timerCallbackForWriteBufferPeriodicFlush --> " + e.getMessage());
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

      // Stop any running Periodic Flush timer.
      disableWriteBufferPeriodicFlush();

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
      try (QueueRowAccess taker = createQueueRowAccess()){
        ap.submit(tableName, taker, true, null, false);
        if (ap.hasError()) {
          LOG.debug(tableName + ": One or more of the operations have failed -"
              + " waiting for all operation in progress to finish (successfully or not)");
        }
      }
    }
    if (synchronous || ap.hasError()) {
      while (true) {
        try (QueueRowAccess taker = createQueueRowAccess()){
          if (taker.isEmpty()) {
            break;
          }
          ap.submit(tableName, taker, true, null, false);
        }
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
   * @deprecated Going away when we drop public support for {@link HTableInterface}.
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

  /**
   * Sets the maximum time before the buffer is automatically flushed.
   * @param timeoutMs    The maximum number of milliseconds how long records may be buffered
   *                     before they are flushed. Set to 0 to disable.
   * @param timerTickMs  The number of milliseconds between each check if the
   *                     timeout has been exceeded. Must be 100ms (as defined in
   *                     {@link #MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS})
   *                     or larger to avoid performance problems.
   */
  public synchronized void setWriteBufferPeriodicFlush(long timeoutMs, long timerTickMs) {
    long originalTimeoutMs   = this.writeBufferPeriodicFlushTimeoutMs.get();
    long originalTimerTickMs = this.writeBufferPeriodicFlushTimerTickMs.get();

    // Both parameters have minimal values.
    writeBufferPeriodicFlushTimeoutMs.set(Math.max(0, timeoutMs));
    writeBufferPeriodicFlushTimerTickMs.set(
            Math.max(MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS, timerTickMs));

    // If something changed we stop the old Timer.
    if (writeBufferPeriodicFlushTimeoutMs.get() != originalTimeoutMs ||
        writeBufferPeriodicFlushTimerTickMs.get() != originalTimerTickMs) {
      if (writeBufferPeriodicFlushTimer != null) {
        writeBufferPeriodicFlushTimer.cancel();
        writeBufferPeriodicFlushTimer = null;
      }
    }

    // If we have the need for a timer and there is none we start it
    if (writeBufferPeriodicFlushTimer == null &&
        writeBufferPeriodicFlushTimeoutMs.get() > 0) {
      writeBufferPeriodicFlushTimer = new Timer(true); // Create Timer running as Daemon.
      writeBufferPeriodicFlushTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          BufferedMutatorImpl.this.timerCallbackForWriteBufferPeriodicFlush();
        }
      }, writeBufferPeriodicFlushTimerTickMs.get(),
         writeBufferPeriodicFlushTimerTickMs.get());
    }
  }

  /**
   * Disable periodic flushing of the write buffer.
   */
  public void disableWriteBufferPeriodicFlush() {
    setWriteBufferPeriodicFlush(0, MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS);
  }

  /**
   * Sets the maximum time before the buffer is automatically flushed checking once per second.
   * @param timeoutMs    The maximum number of milliseconds how long records may be buffered
   *                     before they are flushed. Set to 0 to disable.
   */
  public void setWriteBufferPeriodicFlush(long timeoutMs) {
    setWriteBufferPeriodicFlush(timeoutMs, 1000L);
  }

  /**
   * Returns the current periodic flush timeout value in milliseconds.
   * @return The maximum number of milliseconds how long records may be buffered before they
   *   are flushed. The value 0 means this is disabled.
   */
  public long getWriteBufferPeriodicFlushTimeoutMs() {
    return writeBufferPeriodicFlushTimeoutMs.get();
  }

  /**
   * Returns the current periodic flush timertick interval in milliseconds.
   * @return The number of milliseconds between each check if the timeout has been exceeded.
   *   This value only has a real meaning if the timeout has been set to > 0
   */
  public long getWriteBufferPeriodicFlushTimerTickMs() {
    return writeBufferPeriodicFlushTimerTickMs.get();
  }

  public void setRpcTimeout(int writeRpcTimeout) {
    this.writeRpcTimeout = writeRpcTimeout;
    this.ap.setRpcTimeout(writeRpcTimeout);
  }

  public void setOperationTimeout(int operationTimeout) {
    this.operationTimeout = operationTimeout;
    this.ap.setOperationTimeout(operationTimeout);
  }

  @VisibleForTesting
  long getCurrentWriteBufferSize() {
    return currentWriteBufferSize.get();
  }

  /**
   * This is used for legacy purposes in {@link HTable#getWriteBuffer()} only. This should not beÓ
   * called from production uses.
   * @deprecated Going away when we drop public support for {@link HTableInterface}.
Ó   */
  @Deprecated
  public List<Row> getWriteBuffer() {
    return Arrays.asList(writeAsyncBuffer.toArray(new Row[0]));
  }

  @VisibleForTesting
  QueueRowAccess createQueueRowAccess() {
    return new QueueRowAccess();
  }

  @VisibleForTesting
  class QueueRowAccess implements RowAccess<Row>, Closeable {
    private int remainder = undealtMutationCount.getAndSet(0);
    private Mutation last = null;

    @Override
    public Iterator<Row> iterator() {
      return new Iterator<Row>() {
        private int countDown = remainder;
        @Override
        public boolean hasNext() {
          return countDown > 0;
        }
        @Override
        public Row next() {
          restoreLastMutation();
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          last = writeAsyncBuffer.poll();
          if (last == null) {
            throw new NoSuchElementException();
          }
          currentWriteBufferSize.addAndGet(-last.heapSize());
          --countDown;
          return last;
        }
        @Override
        public void remove() {
          if (last == null) {
            throw new IllegalStateException();
          }
          --remainder;
          last = null;
        }
      };
    }

    private void restoreLastMutation() {
      // restore the last mutation since it isn't submitted
      if (last != null) {
        writeAsyncBuffer.add(last);
        currentWriteBufferSize.addAndGet(last.heapSize());
        last = null;
      }
    }

    @Override
    public int size() {
      return remainder;
    }

    @Override
    public boolean isEmpty() {
      return remainder <= 0;
    }
    @Override
    public void close() {
      restoreLastMutation();
      if (remainder > 0) {
        undealtMutationCount.addAndGet(remainder);
        remainder = 0;
      }
    }
  }
}

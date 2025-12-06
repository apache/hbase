/*
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

import static org.apache.hadoop.hbase.client.ConnectionUtils.validatePut;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;

/**
 * The implementation of {@link AsyncBufferedMutator}. Simply wrap an {@link AsyncTable}.
 */
@InterfaceAudience.Private
class AsyncBufferedMutatorImpl implements AsyncBufferedMutator {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncBufferedMutatorImpl.class);

  private static final int INITIAL_CAPACITY = 100;

  protected static class Batch {
    final ArrayList<Mutation> toSend;
    final ArrayList<CompletableFuture<Void>> toComplete;

    Batch(ArrayList<Mutation> toSend, ArrayList<CompletableFuture<Void>> toComplete) {
      this.toSend = toSend;
      this.toComplete = toComplete;
    }
  }

  private final HashedWheelTimer periodicalFlushTimer;

  private final AsyncTable<?> table;

  private final long writeBufferSize;

  private final long periodicFlushTimeoutNs;

  private final int maxKeyValueSize;

  private final int maxMutations;

  private ArrayList<Mutation> mutations = new ArrayList<>(INITIAL_CAPACITY);

  private ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(INITIAL_CAPACITY);

  private long bufferedSize;

  private volatile boolean closed;

  // Accessed by tests
  Timeout periodicFlushTask;

  // Accessed by tests
  final ReentrantLock lock = new ReentrantLock();

  AsyncBufferedMutatorImpl(HashedWheelTimer periodicalFlushTimer, AsyncTable<?> table,
    long writeBufferSize, long periodicFlushTimeoutNs, int maxKeyValueSize, int maxMutations) {
    this.periodicalFlushTimer = periodicalFlushTimer;
    this.table = table;
    this.writeBufferSize = writeBufferSize;
    this.periodicFlushTimeoutNs = periodicFlushTimeoutNs;
    this.maxKeyValueSize = maxKeyValueSize;
    this.maxMutations = maxMutations;
  }

  @Override
  public TableName getName() {
    return table.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  /**
   * Atomically drains the current buffered mutations and futures under {@link #lock} and prepares
   * this mutator to accept a new batch.
   * <p>
   * The {@link #lock} must be acquired before calling this method. Cancels any pending
   * {@link #periodicFlushTask} to avoid a redundant flush for the data we are about to send. Swaps
   * the shared {@link #mutations} and {@link #futures} lists into a returned {@link Batch},
   * replaces them with fresh lists, and resets {@link #bufferedSize} to zero.
   * <p>
   * If there is nothing buffered, returns {@code null} so callers can skip sending work.
   * <p>
   * Protected for being overridden in tests.
   * @return a {@link Batch} containing drained mutations and futures, or {@code null} if empty
   */
  protected Batch drainBatch() {
    ArrayList<Mutation> toSend;
    ArrayList<CompletableFuture<Void>> toComplete;
    // Cancel the flush task if it is pending.
    if (periodicFlushTask != null) {
      periodicFlushTask.cancel();
      periodicFlushTask = null;
    }
    toSend = this.mutations;
    if (toSend.isEmpty()) {
      return null;
    }
    toComplete = this.futures;
    assert toSend.size() == toComplete.size();
    this.mutations = new ArrayList<>(INITIAL_CAPACITY);
    this.futures = new ArrayList<>(INITIAL_CAPACITY);
    bufferedSize = 0L;
    return new Batch(toSend, toComplete);
  }

  /**
   * Sends a previously drained {@link Batch} and wires the user-visible completion futures to the
   * underlying results returned by {@link AsyncTable#batch(List)}.
   * <p>
   * Preserves the one-to-one, in-order mapping between mutations and their corresponding futures.
   * @param batch the drained batch to send; may be {@code null}
   */
  private void sendBatch(Batch batch) {
    if (batch == null) {
      return;
    }
    Iterator<CompletableFuture<Void>> toCompleteIter = batch.toComplete.iterator();
    for (CompletableFuture<?> future : table.batch(batch.toSend)) {
      CompletableFuture<Void> toCompleteFuture = toCompleteIter.next();
      addListener(future, (r, e) -> {
        if (e != null) {
          toCompleteFuture.completeExceptionally(e);
        } else {
          toCompleteFuture.complete(null);
        }
      });
    }
  }

  @Override
  public List<CompletableFuture<Void>> mutate(List<? extends Mutation> mutations) {
    List<CompletableFuture<Void>> futures = new ArrayList<>(mutations.size());
    for (int i = 0, n = mutations.size(); i < n; i++) {
      futures.add(new CompletableFuture<>());
    }
    if (closed) {
      IOException ioe = new IOException("Already closed");
      futures.forEach(f -> f.completeExceptionally(ioe));
      return futures;
    }
    long heapSize = 0;
    for (Mutation mutation : mutations) {
      heapSize += mutation.heapSize();
      if (mutation instanceof Put) {
        validatePut((Put) mutation, maxKeyValueSize);
      }
    }
    Batch batch = null;
    lock.lock();
    try {
      if (this.mutations.isEmpty() && periodicFlushTimeoutNs > 0) {
        periodicFlushTask = periodicalFlushTimer.newTimeout(timeout -> {
          Batch flushBatch = null;
          lock.lock();
          try {
            // confirm that we are still valid, if there is already an internalFlush call before us,
            // then we should not execute anymore. And in internalFlush we will set periodicFlush
            // to null, and since we may schedule a new one, so here we check whether the references
            // are equal.
            if (timeout == periodicFlushTask) {
              periodicFlushTask = null;
              flushBatch = drainBatch(); // Drains under lock
            }
          } finally {
            lock.unlock();
          }
          if (flushBatch != null) {
            sendBatch(flushBatch); // Sends outside of lock
          }
        }, periodicFlushTimeoutNs, TimeUnit.NANOSECONDS);
      }
      this.mutations.addAll(mutations);
      this.futures.addAll(futures);
      bufferedSize += heapSize;
      if (bufferedSize >= writeBufferSize) {
        LOG.trace("Flushing because write buffer size {} reached", writeBufferSize);
        // drain now and send after releasing the lock
        batch = drainBatch();
      } else if (maxMutations > 0 && this.mutations.size() >= maxMutations) {
        LOG.trace("Flushing because max mutations {} reached", maxMutations);
        batch = drainBatch();
      }
    } finally {
      lock.unlock();
    }
    // Send outside of lock
    if (batch != null) {
      sendBatch(batch);
    }
    return futures;
  }

  // The only difference bewteen flush and close is that, we will set closed to true before sending
  // out the batch to prevent further flush or close
  private void flushOrClose(boolean close) {
    Batch batch = null;
    if (!closed) {
      lock.lock();
      try {
        if (!closed) {
          // Drains under lock
          batch = drainBatch();
          if (close) {
            closed = true;
          }
        }
      } finally {
        lock.unlock();
      }
    }
    // Send the batch
    if (batch != null) {
      // Sends outside of lock
      sendBatch(batch);
    }
  }

  @Override
  public void flush() {
    flushOrClose(false);
  }

  @Override
  public void close() {
    flushOrClose(true);
  }

  @Override
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  @Override
  public long getPeriodicalFlushTimeout(TimeUnit unit) {
    return unit.convert(periodicFlushTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public int getMaxMutations() {
    return maxMutations;
  }

  @Override
  public Map<String, byte[]> getRequestAttributes() {
    return table.getRequestAttributes();
  }
}

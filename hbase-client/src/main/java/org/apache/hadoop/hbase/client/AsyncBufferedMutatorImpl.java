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

  private final int INITIAL_CAPACITY = 100;

  private final HashedWheelTimer periodicalFlushTimer;

  private final AsyncTable<?> table;

  private final long writeBufferSize;

  private final long periodicFlushTimeoutNs;

  private final int maxKeyValueSize;

  private final int maxMutations;

  private final ReentrantLock lock = new ReentrantLock();

  private List<Mutation> mutations = new ArrayList<>(INITIAL_CAPACITY);

  private List<CompletableFuture<Void>> futures = new ArrayList<>(INITIAL_CAPACITY);

  private long bufferedSize;

  private volatile boolean closed;

  Timeout periodicFlushTask;

  enum FlushType {
    /** Flush triggered by buffer size exceeding threshold */
    SIZE,
    /** Flush triggered by max mutations */
    MAX_MUTATIONS,
    /** Flush triggered by periodic timer */
    PERIODIC,
    /** Flush triggered by explicit flush() call */
    MANUAL,
    /** Flush triggered during close() */
    CLOSE
  }

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

  // will be overridden in test
  protected void internalFlush() {
    internalFlush(FlushType.MANUAL);
  }

  protected void internalFlush(FlushType trigger) {
    List<Mutation> toSend;
    List<CompletableFuture<Void>> toComplete;
    // Ensure that the mutations and futures are not modified while we are processing them.
    lock.lock();
    try {
      // Double-check that the condition is still met to avoid unnecessary flushes due to races
      // between size-triggered, max mutations-triggered, manual, and periodic flushes.
      if (trigger == FlushType.SIZE && bufferedSize < writeBufferSize) {
        return;
      } else if (trigger == FlushType.MAX_MUTATIONS && this.mutations.size() < maxMutations) {
        return;
      }
      // Cancel the flush task if it is pending.
      if (periodicFlushTask != null) {
        periodicFlushTask.cancel();
        periodicFlushTask = null;
      }
      toSend = this.mutations;
      if (toSend.isEmpty()) {
        return;
      }
      toComplete = this.futures;
      assert toSend.size() == toComplete.size();
      this.mutations = new ArrayList<>(INITIAL_CAPACITY);
      this.futures = new ArrayList<>(INITIAL_CAPACITY);
      bufferedSize = 0L;
    } finally {
      lock.unlock();
    }
    // Send the mutations and link the futures we returned to our client to the futures we get back
    // from AsyncTable#batch.
    Iterator<CompletableFuture<Void>> toCompleteIter = toComplete.iterator();
    for (CompletableFuture<?> future : table.batch(toSend)) {
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
    boolean needFlush = false;
    FlushType flushType = FlushType.SIZE;
    lock.lock();
    try {
      if (this.mutations.isEmpty() && periodicFlushTimeoutNs > 0) {
        periodicFlushTask = periodicalFlushTimer.newTimeout(timeout -> {
          boolean shouldFlush = false;
          synchronized (AsyncBufferedMutatorImpl.this) {
            // confirm that we are still valid, if there is already an internalFlush call before us,
            // then we should not execute anymore. And in internalFlush we will set periodicFlush
            // to null, and since we may schedule a new one, so here we check whether the references
            // are equal.
            if (timeout == periodicFlushTask) {
              periodicFlushTask = null;
              shouldFlush = true;
            }
          }
          if (shouldFlush) {
            internalFlush(FlushType.PERIODIC);
          }
        }, periodicFlushTimeoutNs, TimeUnit.NANOSECONDS);
      }
      // Preallocate to avoid potentially multiple resizes during addAll if we can.
      if (this.mutations instanceof ArrayList && this.futures instanceof ArrayList) {
        ((ArrayList<Mutation>) this.mutations).ensureCapacity(this.mutations.size()
          + mutations.size());
        ((ArrayList<CompletableFuture<Void>>) this.futures).ensureCapacity(this.futures.size()
          + futures.size());
      }
      this.mutations.addAll(mutations);
      this.futures.addAll(futures);
      bufferedSize += heapSize;
      if (bufferedSize >= writeBufferSize) {
        LOG.trace("Flushing because write buffer size {} reached", writeBufferSize);
        needFlush = true;
        flushType = FlushType.SIZE;
      } else if (this.mutations.size() >= maxMutations) {
        LOG.trace("Flushing because max mutations {} reached", maxMutations);
        needFlush = true;
        flushType = FlushType.MAX_MUTATIONS;
      }
    } finally {
      lock.unlock();
    }
    if (needFlush) {
      internalFlush(flushType);
    }
    return futures;
  }

  @Override
  public void flush() {
    internalFlush();
  }

  @Override
  public void close() {
    boolean needFlush = false;
    if (!closed) {
      lock.lock();
      try {
        if (!closed) {
          closed = true;
          needFlush = true;
        }
      } finally {
        lock.unlock();
      }
    }
    if (needFlush) {
      internalFlush(FlushType.CLOSE);
    }
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

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

import static org.apache.hadoop.hbase.client.ConnectionUtils.validateMutation;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  private final HashedWheelTimer periodicalFlushTimer;

  private final AsyncTable<?> table;

  private final long writeBufferSize;

  private final long periodicFlushTimeoutNs;

  private final int maxKeyValueSize;

  private final int maxMutations;

  private List<Mutation> mutations = new ArrayList<>();

  private List<CompletableFuture<Void>> futures = new ArrayList<>();

  private long bufferedSize;

  private boolean closed;

  Timeout periodicFlushTask;

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
    if (periodicFlushTask != null) {
      periodicFlushTask.cancel();
      periodicFlushTask = null;
    }
    List<Mutation> toSend = this.mutations;
    if (toSend.isEmpty()) {
      return;
    }
    List<CompletableFuture<Void>> toComplete = this.futures;
    assert toSend.size() == toComplete.size();
    this.mutations = new ArrayList<>();
    this.futures = new ArrayList<>();
    bufferedSize = 0L;
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
    List<CompletableFuture<Void>> futures =
      Stream.<CompletableFuture<Void>> generate(CompletableFuture::new).limit(mutations.size())
        .collect(Collectors.toList());
    long heapSize = 0;
    for (Mutation mutation : mutations) {
      heapSize += mutation.heapSize();
      validateMutation(mutation, maxKeyValueSize);
    }
    synchronized (this) {
      if (closed) {
        IOException ioe = new IOException("Already closed");
        futures.forEach(f -> f.completeExceptionally(ioe));
        return futures;
      }
      if (this.mutations.isEmpty() && periodicFlushTimeoutNs > 0) {
        periodicFlushTask = periodicalFlushTimer.newTimeout(timeout -> {
          synchronized (AsyncBufferedMutatorImpl.this) {
            // confirm that we are still valid, if there is already an internalFlush call before us,
            // then we should not execute anymore. And in internalFlush we will set periodicFlush
            // to null, and since we may schedule a new one, so here we check whether the references
            // are equal.
            if (timeout == periodicFlushTask) {
              periodicFlushTask = null;
              internalFlush();
            }
          }
        }, periodicFlushTimeoutNs, TimeUnit.NANOSECONDS);
      }
      this.mutations.addAll(mutations);
      this.futures.addAll(futures);
      bufferedSize += heapSize;
      if (bufferedSize >= writeBufferSize) {
        LOG.trace("Flushing because write buffer size {} reached", writeBufferSize);
        internalFlush();
      } else if (maxMutations > 0 && this.mutations.size() >= maxMutations) {
        LOG.trace("Flushing because max mutations {} reached", maxMutations);
        internalFlush();
      }
    }
    return futures;
  }

  @Override
  public synchronized void flush() {
    internalFlush();
  }

  @Override
  public synchronized void close() {
    internalFlush();
    closed = true;
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

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
package org.apache.hadoop.hbase.util;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A ConcurrentLinkedQueue that enforces a maximum queue size.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class BoundedConcurrentLinkedQueue<T> extends ConcurrentLinkedQueue<T> {
  private static final long serialVersionUID = 1L;
  private final AtomicLong size = new AtomicLong(0L);
  private final long maxSize;

  public BoundedConcurrentLinkedQueue() {
    this(Long.MAX_VALUE);
  }

  public BoundedConcurrentLinkedQueue(long maxSize) {
    super();
    this.maxSize = maxSize;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    for (;;) {
      long currentSize = size.get();
      long nextSize = currentSize + c.size();
      if (nextSize > maxSize) { // already exceeded limit
        return false;
      }
      if (size.compareAndSet(currentSize, nextSize)) {
        break;
      }
    }
    return super.addAll(c); // Always true for ConcurrentLinkedQueue
  }

  @Override
  public void clear() {
    // override this method to batch update size.
    long removed = 0L;
    while (super.poll() != null) {
      removed++;
    }
    size.addAndGet(-removed);
  }

  @Override
  public boolean offer(T e) {
    for (;;) {
      long currentSize = size.get();
      if (currentSize >= maxSize) { // already exceeded limit
        return false;
      }
      if (size.compareAndSet(currentSize, currentSize + 1)) {
        break;
      }
    }
    return super.offer(e); // Always true for ConcurrentLinkedQueue
  }

  @Override
  public T poll() {
    T result = super.poll();
    if (result != null) {
      size.decrementAndGet();
    }
    return result;
  }

  @Override
  public boolean remove(Object o) {
    boolean result = super.remove(o);
    if (result) {
      size.decrementAndGet();
    }
    return result;
  }

  @Override
  public int size() {
    return (int) size.get();
  }

  public void drainTo(Collection<T> list) {
    long removed = 0;
    for (T element; (element = super.poll()) != null;) {
      list.add(element);
      removed++;
    }
    // Limit the number of operations on size by only reporting size change after the drain is
    // completed.
    size.addAndGet(-removed);
  }

  public long remainingCapacity() {
    return maxSize - size.get();
  }
}
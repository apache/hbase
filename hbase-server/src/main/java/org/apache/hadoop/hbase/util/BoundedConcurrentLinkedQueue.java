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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A ConcurrentLinkedQueue that enforces a maximum queue size.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class BoundedConcurrentLinkedQueue<T> extends ConcurrentLinkedQueue<T> {
  private static final long serialVersionUID = 1L;
  private volatile long size = 0;
  private final long maxSize;

  public BoundedConcurrentLinkedQueue() {
    this(Long.MAX_VALUE);
  }

  public BoundedConcurrentLinkedQueue(long maxSize) {
    super();
    this.maxSize = maxSize;
  }

  @Override
  public boolean add(T e) {
    return offer(e);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    size += c.size();        // Between here and below we might reject offers,
    if (size > maxSize) {    // if over maxSize, but that's ok
      size -= c.size();      // We're over, just back out and return.
      return false;
    }
    return super.addAll(c);  // Always true for ConcurrentLinkedQueue
  }

  @Override
  public void clear() {
    super.clear();
    size = 0;
  }

  @Override
  public boolean offer(T e) {
    if (++size > maxSize) {
      --size;                // We didn't take it after all
      return false;
    }
    return super.offer(e);   // Always true for ConcurrentLinkedQueue
  }

  @Override
  public T poll() {
    T result = super.poll();
    if (result != null) {
      --size;
    }
    return result;
  }

  @Override
  public boolean remove(Object o) {
    boolean result = super.remove(o);
    if (result) {
      --size;
    }
    return result;
  }

  @Override
  public int size() {
    return (int) size;
  }

  public void drainTo(Collection<T> list) {
    long removed = 0;
    T l;
    while ((l = super.poll()) != null) {
      list.add(l);
      removed++;
    }
    // Limit the number of operations on a volatile by only reporting size
    // change after the drain is completed.
    size -= removed;
  }

  public long remainingCapacity() {
    long remaining = maxSize - size;
    return remaining >= 0 ? remaining : 0;
  }
}
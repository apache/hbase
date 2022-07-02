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
package org.apache.hadoop.hbase.master.normalizer;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A specialized collection that holds pending work for the {@link RegionNormalizerWorker}. It is an
 * ordered collection class that has the following properties:
 * <ul>
 * <li>Guarantees uniqueness of elements, as a {@link Set}.</li>
 * <li>Consumers retrieve objects from the head, as a {@link Queue}, via {@link #take()}.</li>
 * <li>Work is retrieved on a FIFO policy.</li>
 * <li>Work retrieval blocks the calling thread until new work is available, as a
 * {@link BlockingQueue}.</li>
 * <li>Allows a producer to insert an item at the head of the queue, if desired.</li>
 * </ul>
 */
@InterfaceAudience.Private
class RegionNormalizerWorkQueue<E> {

  /** Underlying storage structure that gives us the Set behavior and FIFO retrieval policy. */
  private LinkedHashSet<E> delegate;

  /** Lock for puts and takes **/
  private final ReentrantReadWriteLock lock;
  /** Wait queue for waiting takes */
  private final Condition notEmpty;

  RegionNormalizerWorkQueue() {
    delegate = new LinkedHashSet<>();
    lock = new ReentrantReadWriteLock();
    notEmpty = lock.writeLock().newCondition();
  }

  /**
   * Inserts the specified element at the tail of the queue, if it's not already present.
   * @param e the element to add
   */
  public void put(E e) {
    if (e == null) {
      throw new NullPointerException();
    }
    lock.writeLock().lock();
    try {
      delegate.add(e);
      if (!delegate.isEmpty()) {
        notEmpty.signal();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Inserts the specified element at the head of the queue.
   * @param e the element to add
   */
  public void putFirst(E e) {
    if (e == null) {
      throw new NullPointerException();
    }
    putAllFirst(Collections.singleton(e));
  }

  /**
   * Inserts the specified elements at the tail of the queue. Any elements already present in the
   * queue are ignored.
   * @param c the elements to add
   */
  public void putAll(Collection<? extends E> c) {
    if (c == null) {
      throw new NullPointerException();
    }
    lock.writeLock().lock();
    try {
      delegate.addAll(c);
      if (!delegate.isEmpty()) {
        notEmpty.signal();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Inserts the specified elements at the head of the queue.
   * @param c the elements to add
   */
  public void putAllFirst(Collection<? extends E> c) {
    if (c == null) {
      throw new NullPointerException();
    }
    lock.writeLock().lock();
    try {
      final LinkedHashSet<E> copy = new LinkedHashSet<>(c.size() + delegate.size());
      copy.addAll(c);
      copy.addAll(delegate);
      delegate = copy;
      if (!delegate.isEmpty()) {
        notEmpty.signal();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Retrieves and removes the head of this queue, waiting if necessary until an element becomes
   * available.
   * @return the head of this queue
   * @throws InterruptedException if interrupted while waiting
   */
  public E take() throws InterruptedException {
    E x;
    // Take a write lock. If the delegate's queue is empty we need it to await(), which will
    // drop the lock, then reacquire it; or if the queue is not empty we will use an iterator
    // to mutate the head.
    lock.writeLock().lockInterruptibly();
    try {
      while (delegate.isEmpty()) {
        notEmpty.await(); // await drops the lock, then reacquires it
      }
      final Iterator<E> iter = delegate.iterator();
      x = iter.next();
      iter.remove();
      if (!delegate.isEmpty()) {
        notEmpty.signal();
      }
    } finally {
      lock.writeLock().unlock();
    }
    return x;
  }

  /**
   * Atomically removes all of the elements from this queue. The queue will be empty after this call
   * returns.
   */
  public void clear() {
    lock.writeLock().lock();
    try {
      delegate.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the number of elements in this queue.
   * @return the number of elements in this queue
   */
  public int size() {
    lock.readLock().lock();
    try {
      return delegate.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    lock.readLock().lock();
    try {
      return delegate.toString();
    } finally {
      lock.readLock().unlock();
    }
  }
}

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
import java.util.concurrent.locks.ReentrantLock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A specialized collection that holds pending work for the {@link RegionNormalizerWorker}. It is
 * an ordered collection class that has the following properties:
 * <ul>
 *   <li>Guarantees uniqueness of elements, as a {@link Set}.</li>
 *   <li>Consumers retrieve objects from the head, as a {@link Queue}, via {@link #take()}.</li>
 *   <li>Work is retrieved on a FIFO policy.</li>
 *   <li>Work retrieval blocks the calling thread until new work is available, as a
 *     {@link BlockingQueue}.</li>
 *   <li>Allows a producer to insert an item at the head of the queue, if desired.</li>
 * </ul>
 * Assumes low-frequency and low-parallelism concurrent access, so protects state using a
 * simplistic synchronization strategy.
 */
@InterfaceAudience.Private
class RegionNormalizerWorkQueue<E> {

  /** Underlying storage structure that gives us the Set behavior and FIFO retrieval policy. */
  private LinkedHashSet<E> delegate;

  // the locking structure used here follows the example found in LinkedBlockingQueue. The
  // difference is that our locks guard access to `delegate` rather than the head node.

  /** Lock held by take, poll, etc */
  private final ReentrantLock takeLock;

  /** Wait queue for waiting takes */
  private final Condition notEmpty;

  /** Lock held by put, offer, etc */
  private final ReentrantLock putLock;

  RegionNormalizerWorkQueue() {
    delegate = new LinkedHashSet<>();
    takeLock = new ReentrantLock();
    notEmpty = takeLock.newCondition();
    putLock = new ReentrantLock();
  }

  /**
   * Signals a waiting take. Called only from put/offer (which do not
   * otherwise ordinarily lock takeLock.)
   */
  private void signalNotEmpty() {
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lock();
    try {
      notEmpty.signal();
    } finally {
      takeLock.unlock();
    }
  }

  /**
   * Locks to prevent both puts and takes.
   */
  private void fullyLock() {
    putLock.lock();
    takeLock.lock();
  }

  /**
   * Unlocks to allow both puts and takes.
   */
  private void fullyUnlock() {
    takeLock.unlock();
    putLock.unlock();
  }

  /**
   * Inserts the specified element at the tail of the queue, if it's not already present.
   *
   * @param e the element to add
   */
  public void put(E e) {
    if (e == null) {
      throw new NullPointerException();
    }

    putLock.lock();
    try {
      delegate.add(e);
    } finally {
      putLock.unlock();
    }

    if (!delegate.isEmpty()) {
      signalNotEmpty();
    }
  }

  /**
   * Inserts the specified element at the head of the queue.
   *
   * @param e the element to add
   */
  public void putFirst(E e) {
    if (e == null) {
      throw new NullPointerException();
    }
    putAllFirst(Collections.singleton(e));
  }

  /**
   * Inserts the specified elements at the tail of the queue. Any elements already present in
   * the queue are ignored.
   *
   * @param c the elements to add
   */
  public void putAll(Collection<? extends E> c) {
    if (c == null) {
      throw new NullPointerException();
    }

    putLock.lock();
    try {
      delegate.addAll(c);
    } finally {
      putLock.unlock();
    }

    if (!delegate.isEmpty()) {
      signalNotEmpty();
    }
  }

  /**
   * Inserts the specified elements at the head of the queue.
   *
   * @param c the elements to add
   */
  public void putAllFirst(Collection<? extends E> c) {
    if (c == null) {
      throw new NullPointerException();
    }

    fullyLock();
    try {
      final LinkedHashSet<E> copy = new LinkedHashSet<>(c.size() + delegate.size());
      copy.addAll(c);
      copy.addAll(delegate);
      delegate = copy;
    } finally {
      fullyUnlock();
    }

    if (!delegate.isEmpty()) {
      signalNotEmpty();
    }
  }

  /**
   * Retrieves and removes the head of this queue, waiting if necessary
   * until an element becomes available.
   *
   * @return the head of this queue
   * @throws InterruptedException if interrupted while waiting
   */
  public E take() throws InterruptedException {
    E x;
    takeLock.lockInterruptibly();
    try {
      while (delegate.isEmpty()) {
        notEmpty.await();
      }
      final Iterator<E> iter = delegate.iterator();
      x = iter.next();
      iter.remove();
      if (!delegate.isEmpty()) {
        notEmpty.signal();
      }
    } finally {
      takeLock.unlock();
    }
    return x;
  }

  /**
   * Atomically removes all of the elements from this queue.
   * The queue will be empty after this call returns.
   */
  public void clear() {
    putLock.lock();
    try {
      delegate.clear();
    } finally {
      putLock.unlock();
    }
  }

  /**
   * Returns the number of elements in this queue.
   *
   * @return the number of elements in this queue
   */
  public int size() {
    takeLock.lock();
    try {
      return delegate.size();
    } finally {
      takeLock.unlock();
    }
  }

  @Override
  public String toString() {
    takeLock.lock();
    try {
      return delegate.toString();
    } finally {
      takeLock.unlock();
    }
  }
}

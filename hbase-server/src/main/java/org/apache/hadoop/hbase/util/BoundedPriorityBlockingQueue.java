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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.AbstractQueue;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;


/**
 * A generic bounded blocking Priority-Queue.
 *
 * The elements of the priority queue are ordered according to the Comparator
 * provided at queue construction time.
 *
 * If multiple elements have the same priority this queue orders them in
 * FIFO (first-in-first-out) manner.
 * The head of this queue is the least element with respect to the specified
 * ordering. If multiple elements are tied for least value, the head is the
 * first one inserted.
 * The queue retrieval operations poll, remove, peek, and element access the
 * element at the head of the queue.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class BoundedPriorityBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
  private static class PriorityQueue<E> {
    private final Comparator<? super E> comparator;
    private final E[] objects;

    private int head = 0;
    private int tail = 0;

    @SuppressWarnings("unchecked")
    public PriorityQueue(int capacity, Comparator<? super E> comparator) {
      this.objects = (E[])new Object[capacity];
      this.comparator = comparator;
    }

    public void add(E elem) {
      if (tail == objects.length) {
        // shift down |-----AAAAAAA|
        tail -= head;
        System.arraycopy(objects, head, objects, 0, tail);
        head = 0;
      }

      if (tail == head || comparator.compare(objects[tail - 1], elem) <= 0) {
        // Append
        objects[tail++] = elem;
      } else if (head > 0 && comparator.compare(objects[head], elem) > 0) {
        // Prepend
        objects[--head] = elem;
      } else {
        // Insert in the middle
        int index = upperBound(head, tail - 1, elem);
        System.arraycopy(objects, index, objects, index + 1, tail - index);
        objects[index] = elem;
        tail++;
      }
    }

    public E peek() {
      return (head != tail) ? objects[head] : null;
    }

    public E poll() {
      E elem = objects[head];
      objects[head] = null;
      head = (head + 1) % objects.length;
      if (head == 0) tail = 0;
      return elem;
    }

    public int size() {
      return tail - head;
    }

    public Comparator<? super E> comparator() {
      return this.comparator;
    }

    public boolean contains(Object o) {
      for (int i = head; i < tail; ++i) {
        if (objects[i] == o) {
          return true;
        }
      }
      return false;
    }

    public int remainingCapacity() {
      return this.objects.length - (tail - head);
    }

    private int upperBound(int start, int end, E key) {
      while (start < end) {
        int mid = (start + end) >>> 1;
        E mitem = objects[mid];
        int cmp = comparator.compare(mitem, key);
        if (cmp > 0) {
          end = mid;
        } else {
          start = mid + 1;
        }
      }
      return start;
    }
  }


  // Lock used for all operations
  private final ReentrantLock lock = new ReentrantLock();

  // Condition for blocking when empty
  private final Condition notEmpty = lock.newCondition();

  // Wait queue for waiting puts
  private final Condition notFull = lock.newCondition();

  private final PriorityQueue<E> queue;

  /**
   * Creates a PriorityQueue with the specified capacity that orders its
   * elements according to the specified comparator.
   * @param capacity the capacity of this queue
   * @param comparator the comparator that will be used to order this priority queue
   */
  public BoundedPriorityBlockingQueue(int capacity,
      Comparator<? super E> comparator) {
    this.queue = new PriorityQueue<E>(capacity, comparator);
  }

  public boolean offer(E e) {
    if (e == null) throw new NullPointerException();

    lock.lock();
    try {
      if (queue.remainingCapacity() > 0) {
        this.queue.add(e);
        notEmpty.signal();
        return true;
      }
    } finally {
      lock.unlock();
    }
    return false;
  }

  public void put(E e) throws InterruptedException {
    if (e == null) throw new NullPointerException();

    lock.lock();
    try {
      while (queue.remainingCapacity() == 0) {
        notFull.await();
      }
      this.queue.add(e);
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (e == null) throw new NullPointerException();
    long nanos = unit.toNanos(timeout);

    lock.lockInterruptibly();
    try {
      while (queue.remainingCapacity() == 0) {
        if (nanos <= 0)
          return false;
        nanos = notFull.awaitNanos(nanos);
      }
      this.queue.add(e);
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
    return true;
  }

  public E take() throws InterruptedException {
    E result = null;
    lock.lockInterruptibly();
    try {
      while (queue.size() == 0) {
        notEmpty.await();
      }
      result = queue.poll();
      notFull.signal();
    } finally {
      lock.unlock();
    }
    return result;
  }

  public E poll() {
    E result = null;
    lock.lock();
    try {
      if (queue.size() > 0) {
        result = queue.poll();
        notFull.signal();
      }
    } finally {
      lock.unlock();
    }
    return result;
  }

  public E poll(long timeout, TimeUnit unit)
      throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    E result = null;
    try {
      while (queue.size() == 0 && nanos > 0) {
        nanos = notEmpty.awaitNanos(nanos);
      }
      if (queue.size() > 0) {
        result = queue.poll();
      }
      notFull.signal();
    } finally {
      lock.unlock();
    }
    return result;
  }

  public E peek() {
    lock.lock();
    try {
      return queue.peek();
    } finally {
      lock.unlock();
    }
  }

  public int size() {
    lock.lock();
    try {
      return queue.size();
    } finally {
      lock.unlock();
    }
  }

  public Iterator<E> iterator() {
    throw new UnsupportedOperationException();
  }

  public Comparator<? super E> comparator() {
    return queue.comparator();
  }

  public int remainingCapacity() {
    lock.lock();
    try {
      return queue.remainingCapacity();
    } finally {
      lock.unlock();
    }
  }

  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  public boolean contains(Object o) {
    lock.lock();
    try {
      return queue.contains(o);
    } finally {
      lock.unlock();
    }
  }

  public int drainTo(Collection<? super E> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  public int drainTo(Collection<? super E> c, int maxElements) {
    if (c == null)
        throw new NullPointerException();
    if (c == this)
        throw new IllegalArgumentException();
    if (maxElements <= 0)
        return 0;
    lock.lock();
    try {
      int n = Math.min(queue.size(), maxElements);
      for (int i = 0; i < n; ++i) {
        c.add(queue.poll());
      }
      return n;
    } finally {
      lock.unlock();
    }
  }
}

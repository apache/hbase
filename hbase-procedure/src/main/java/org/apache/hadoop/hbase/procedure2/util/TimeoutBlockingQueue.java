/**
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

package org.apache.hadoop.hbase.procedure2.util;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TimeoutBlockingQueue<E> {
  public static interface TimeoutRetriever<T> {
    long getTimeout(T object);
    TimeUnit getTimeUnit(T object);
  }

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final TimeoutRetriever<? super E> timeoutRetriever;

  private E[] objects;
  private int head = 0;
  private int tail = 0;

  public TimeoutBlockingQueue(TimeoutRetriever<? super E> timeoutRetriever) {
    this(32, timeoutRetriever);
  }

  @SuppressWarnings("unchecked")
  public TimeoutBlockingQueue(int capacity, TimeoutRetriever<? super E> timeoutRetriever) {
    this.objects = (E[])new Object[capacity];
    this.timeoutRetriever = timeoutRetriever;
  }

  public void dump() {
    for (int i = 0; i < objects.length; ++i) {
      if (i == head) {
        System.out.print("[" + objects[i] + "] ");
      } else if (i == tail) {
        System.out.print("]" + objects[i] + "[ ");
      } else {
        System.out.print(objects[i] + " ");
      }
    }
    System.out.println();
  }

  public void clear() {
    lock.lock();
    try {
      if (head != tail) {
        for (int i = head; i < tail; ++i) {
          objects[i] = null;
        }
        head = 0;
        tail = 0;
        waitCond.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  public void add(E e) {
    if (e == null) throw new NullPointerException();

    lock.lock();
    try {
      addElement(e);
      waitCond.signal();
    } finally {
      lock.unlock();
    }
  }

  public void remove(E e) {
    lock.lock();
    try {
      for (int i = 0; i < objects.length; ++i) {
        if (objects[i] == e) {
          objects[i] = null;
          return;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  public E poll() {
    lock.lock();
    try {
      if (isEmpty()) {
        waitCond.await();
        return null;
      }

      E elem = objects[head];
      long nanos = getNanosTimeout(elem);
      nanos = waitCond.awaitNanos(nanos);
      return nanos > 0 ? null : removeFirst();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } finally {
      lock.unlock();
    }
  }

  public int size() {
    return tail - head;
  }

  public boolean isEmpty() {
    return (tail - head) == 0;
  }

  public void signalAll() {
    lock.lock();
    try {
      waitCond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void addElement(E elem) {
    int size = (tail - head);
    if ((objects.length - size) == 0) {
      int capacity = size + ((size < 64) ? (size + 2) : (size >> 1));
      E[] newObjects = (E[])new Object[capacity];

      if (compareTimeouts(objects[tail - 1], elem) <= 0) {
        // Append
        System.arraycopy(objects, head, newObjects, 0, tail);
        tail -= head;
        newObjects[tail++] = elem;
      } else if (compareTimeouts(objects[head], elem) > 0) {
        // Prepend
        System.arraycopy(objects, head, newObjects, 1, tail);
        newObjects[0] = elem;
        tail -= (head - 1);
      } else {
        // Insert in the middle
        int index = upperBound(head, tail - 1, elem);
        int newIndex = (index - head);
        System.arraycopy(objects, head, newObjects, 0, newIndex);
        newObjects[newIndex] = elem;
        System.arraycopy(objects, index, newObjects, newIndex + 1, tail - index);
        tail -= (head - 1);
      }
      head = 0;
      objects = newObjects;
    } else {
      if (tail == objects.length) {
        // shift down |-----AAAAAAA|
        tail -= head;
        System.arraycopy(objects, head, objects, 0, tail);
        head = 0;
      }

      if (tail == head || compareTimeouts(objects[tail - 1], elem) <= 0) {
        // Append
        objects[tail++] = elem;
      } else if (head > 0 && compareTimeouts(objects[head], elem) > 0) {
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
  }

  private E removeFirst() {
    E elem = objects[head];
    objects[head] = null;
    head = (head + 1) % objects.length;
    if (head == 0) tail = 0;
    return elem;
  }

  private int upperBound(int start, int end, E key) {
    while (start < end) {
      int mid = (start + end) >>> 1;
      E mitem = objects[mid];
      int cmp = compareTimeouts(mitem, key);
      if (cmp > 0) {
        end = mid;
      } else {
        start = mid + 1;
      }
    }
    return start;
  }

  private int compareTimeouts(final E a, final E b) {
    long t1 = getNanosTimeout(a);
    long t2 = getNanosTimeout(b);
    return (t1 < t2) ? -1 : (t1 > t2) ? 1 : 0;
  }

  private long getNanosTimeout(final E obj) {
    if (obj == null) return 0;
    TimeUnit unit = timeoutRetriever.getTimeUnit(obj);
    long timeout = timeoutRetriever.getTimeout(obj);
    return unit.toNanos(timeout);
  }
}

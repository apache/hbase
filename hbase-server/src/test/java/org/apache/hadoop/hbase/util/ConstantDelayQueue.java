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

package org.apache.hadoop.hbase.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A blocking queue implementation for adding a constant delay. Uses a DelayQueue as a backing store
 * @param <E> type of elements
 */
@InterfaceAudience.Private
public class ConstantDelayQueue<E> implements BlockingQueue<E> {

  private static final class DelayedElement<T> implements Delayed {
    T element;
    long end;
    public DelayedElement(T element, long delayMs) {
      this.element = element;
      this.end = EnvironmentEdgeManager.currentTime() + delayMs;
    }

    @Override
    public int compareTo(Delayed o) {
      long cmp = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
      return cmp == 0 ? 0 : ( cmp < 0 ? -1 : 1);
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(end - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
  }

  private final long delayMs;

  // backing DelayQueue
  private DelayQueue<DelayedElement<E>> queue = new DelayQueue<>();

  public ConstantDelayQueue(TimeUnit timeUnit, long delay) {
    this.delayMs = TimeUnit.MILLISECONDS.convert(delay, timeUnit);
  }

  @Override
  public E remove() {
    DelayedElement<E> el = queue.remove();
    return el == null ? null : el.element;
  }

  @Override
  public E poll() {
    DelayedElement<E> el = queue.poll();
    return el == null ? null : el.element;
  }

  @Override
  public E element() {
    DelayedElement<E> el = queue.element();
    return el == null ? null : el.element;
  }

  @Override
  public E peek() {
    DelayedElement<E> el = queue.peek();
    return el == null ? null : el.element;
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public Iterator<E> iterator() {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public boolean add(E e) {
    return queue.add(new DelayedElement<>(e, delayMs));
  }

  @Override
  public boolean offer(E e) {
    return queue.offer(new DelayedElement<>(e, delayMs));
  }

  @Override
  public void put(E e) throws InterruptedException {
    queue.put(new DelayedElement<>(e, delayMs));
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    return queue.offer(new DelayedElement<>(e, delayMs), timeout, unit);
  }

  @Override
  public E take() throws InterruptedException {
    DelayedElement<E> el = queue.take();
    return el == null ? null : el.element;
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    DelayedElement<E> el = queue.poll(timeout, unit);
    return el == null ? null : el.element;
  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    throw new UnsupportedOperationException(); // not implemented yet
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    throw new UnsupportedOperationException(); // not implemented yet
  }
}

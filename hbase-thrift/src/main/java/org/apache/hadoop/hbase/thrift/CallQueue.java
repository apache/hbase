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

package org.apache.hadoop.hbase.thrift;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A BlockingQueue reports waiting time in queue and queue length to
 * ThriftMetrics.
 */
@InterfaceAudience.Private
public class CallQueue implements BlockingQueue<Runnable> {
  private static Log LOG = LogFactory.getLog(CallQueue.class);

  private final BlockingQueue<Call> underlyingQueue;
  private final ThriftMetrics metrics;

  public CallQueue(BlockingQueue<Call> underlyingQueue,
                   ThriftMetrics metrics) {
    this.underlyingQueue = underlyingQueue;
    this.metrics = metrics;
  }

  private static long now() {
    return System.nanoTime();
  }

  public static class Call implements Runnable {
    final long startTime;
    final Runnable underlyingRunnable;

    Call(Runnable underlyingRunnable) {
      this.underlyingRunnable = underlyingRunnable;
      this.startTime = now();
    }

    @Override
    public void run() {
      underlyingRunnable.run();
    }

    public long timeInQueue() {
      return now() - startTime;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Call) {
        Call otherCall = (Call)(other);
        return this.underlyingRunnable.equals(otherCall.underlyingRunnable);
      } else if (other instanceof Runnable) {
        return this.underlyingRunnable.equals(other);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return this.underlyingRunnable.hashCode();
    }
  }

  @Override
  public Runnable poll() {
    Call result = underlyingQueue.poll();
    updateMetrics(result);
    return result;
  }

  private void updateMetrics(Call result) {
    if (result == null) {
      return;
    }
    metrics.incTimeInQueue(result.timeInQueue());
    metrics.setCallQueueLen(this.size());
  }

  @Override
  public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
    Call result = underlyingQueue.poll(timeout, unit);
    updateMetrics(result);
    return result;
  }

  @Override
  public Runnable remove() {
    Call result = underlyingQueue.remove();
    updateMetrics(result);
    return result;
  }

  @Override
  public Runnable take() throws InterruptedException {
    Call result = underlyingQueue.take();
    updateMetrics(result);
    return result;
  }

  @Override
  public int drainTo(Collection<? super Runnable> destination) {
    return drainTo(destination, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(Collection<? super Runnable> destination,
                     int maxElements) {
    if (destination == this) {
      throw new IllegalArgumentException(
          "A BlockingQueue cannot drain to itself.");
    }
    List<Call> drained = new ArrayList<Call>();
    underlyingQueue.drainTo(drained, maxElements);
    for (Call r : drained) {
      updateMetrics(r);
    }
    destination.addAll(drained);
    int sz = drained.size();
    LOG.info("Elements drained: " + sz);
    return sz;
  }


  @Override
  public boolean offer(Runnable element) {
    return underlyingQueue.offer(new Call(element));
  }

  @Override
  public boolean offer(Runnable element, long timeout, TimeUnit unit)
      throws InterruptedException {
    return underlyingQueue.offer(new Call(element), timeout, unit);
  }
  @Override
  public void put(Runnable element) throws InterruptedException {
    underlyingQueue.put(new Call(element));
  }

  @Override
  public boolean add(Runnable element) {
    return underlyingQueue.add(new Call(element));
  }

  @Override
  public boolean addAll(Collection<? extends Runnable> elements) {
    int added = 0;
    for (Runnable r : elements) {
      added += underlyingQueue.add(new Call(r)) ? 1 : 0;
    }
    return added != 0;
  }

  @Override
  public Runnable element() {
    return underlyingQueue.element();
  }

  @Override
  public Runnable peek() {
    return underlyingQueue.peek();
  }

  @Override
  public void clear() {
    underlyingQueue.clear();
  }

  @Override
  public boolean containsAll(Collection<?> elements) {
    return underlyingQueue.containsAll(elements);
  }

  @Override
  public boolean isEmpty() {
    return underlyingQueue.isEmpty();
  }

  @Override
  public Iterator<Runnable> iterator() {
    return new Iterator<Runnable>() {
      final Iterator<Call> underlyingIterator = underlyingQueue.iterator();
      @Override
      public Runnable next() {
        return underlyingIterator.next();
      }

      @Override
      public boolean hasNext() {
        return underlyingIterator.hasNext();
      }

      @Override
      public void remove() {
        underlyingIterator.remove();
      }
    };
  }

  @Override
  public boolean removeAll(Collection<?> elements) {
    return underlyingQueue.removeAll(elements);
  }

  @Override
  public boolean retainAll(Collection<?> elements) {
    return underlyingQueue.retainAll(elements);
  }

  @Override
  public int size() {
    return underlyingQueue.size();
  }

  @Override
  public Object[] toArray() {
    return underlyingQueue.toArray();
  }

  @Override
  public <T> T[] toArray(T[] array) {
    return underlyingQueue.toArray(array);
  }

  @Override
  public boolean contains(Object element) {
    return underlyingQueue.contains(element);
  }

  @Override
  public int remainingCapacity() {
    return underlyingQueue.remainingCapacity();
  }

  @Override
  public boolean remove(Object element) {
    return underlyingQueue.remove(element);
  }
}

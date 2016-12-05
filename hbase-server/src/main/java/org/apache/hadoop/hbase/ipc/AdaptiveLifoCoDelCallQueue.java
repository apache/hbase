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
package org.apache.hadoop.hbase.ipc;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Adaptive LIFO blocking queue utilizing CoDel algorithm to prevent queue overloading.
 *
 * Implementing {@link BlockingQueue} interface to be compatible with {@link RpcExecutor}.
 *
 * Currently uses milliseconds internally, need to look into whether we should use
 * nanoseconds for timeInterval and minDelay.
 *
 * @see <a href="http://queue.acm.org/detail.cfm?id=2839461">Fail at Scale paper</a>
 *
 * @see <a href="https://github.com/facebook/wangle/blob/master/wangle/concurrent/Codel.cpp">
 *   CoDel version for generic job queues in Wangle library</a>
 */
@InterfaceAudience.Private
public class AdaptiveLifoCoDelCallQueue implements BlockingQueue<CallRunner> {

  // backing queue
  private LinkedBlockingDeque<CallRunner> queue;

  // so we can calculate actual threshold to switch to LIFO under load
  private int maxCapacity;

  // metrics (shared across all queues)
  private AtomicLong numGeneralCallsDropped;
  private AtomicLong numLifoModeSwitches;

  // Both are in milliseconds
  private volatile int codelTargetDelay;
  private volatile int codelInterval;

  // if queue if full more than that percent, we switch to LIFO mode.
  // Values are in the range of 0.7, 0.8 etc (0-1.0).
  private volatile double lifoThreshold;

  // minimal delay observed during the interval
  private volatile long minDelay;

  // the moment when current interval ends
  private volatile long intervalTime = EnvironmentEdgeManager.currentTime();

  // switch to ensure only one threads does interval cutoffs
  private AtomicBoolean resetDelay = new AtomicBoolean(true);

  // if we're in this mode, "long" calls are getting dropped
  private AtomicBoolean isOverloaded = new AtomicBoolean(false);

  public AdaptiveLifoCoDelCallQueue(int capacity, int targetDelay, int interval,
      double lifoThreshold, AtomicLong numGeneralCallsDropped, AtomicLong numLifoModeSwitches) {
    this.maxCapacity = capacity;
    this.queue = new LinkedBlockingDeque<>(capacity);
    this.codelTargetDelay = targetDelay;
    this.codelInterval = interval;
    this.lifoThreshold = lifoThreshold;
    this.numGeneralCallsDropped = numGeneralCallsDropped;
    this.numLifoModeSwitches = numLifoModeSwitches;
  }

  /**
   * Update tunables.
   *
   * @param newCodelTargetDelay new CoDel target delay
   * @param newCodelInterval new CoDel interval
   * @param newLifoThreshold new Adaptive Lifo threshold
   */
  public void updateTunables(int newCodelTargetDelay, int newCodelInterval,
                             double newLifoThreshold) {
    this.codelTargetDelay = newCodelTargetDelay;
    this.codelInterval = newCodelInterval;
    this.lifoThreshold = newLifoThreshold;
  }

  /**
   * Behaves as {@link LinkedBlockingQueue#take()}, except it will silently
   * skip all calls which it thinks should be dropped.
   *
   * @return the head of this queue
   * @throws InterruptedException if interrupted while waiting
   */
  @Override
  public CallRunner take() throws InterruptedException {
    CallRunner cr;
    while(true) {
      if (((double) queue.size() / this.maxCapacity) > lifoThreshold) {
        numLifoModeSwitches.incrementAndGet();
        cr = queue.takeLast();
      } else {
        cr = queue.takeFirst();
      }
      if (needToDrop(cr)) {
        numGeneralCallsDropped.incrementAndGet();
        cr.drop();
      } else {
        return cr;
      }
    }
  }

  @Override
  public CallRunner poll() {
    CallRunner cr;
    boolean switched = false;
    while(true) {
      if (((double) queue.size() / this.maxCapacity) > lifoThreshold) {
        // Only count once per switch.
        if (!switched) {
          switched = true;
          numLifoModeSwitches.incrementAndGet();
        }
        cr = queue.pollLast();
      } else {
        switched = false;
        cr = queue.pollFirst();
      }
      if (cr == null) {
        return cr;
      }
      if (needToDrop(cr)) {
        numGeneralCallsDropped.incrementAndGet();
        cr.drop();
      } else {
        return cr;
      }
    }
  }

  /**
   * @param callRunner to validate
   * @return true if this call needs to be skipped based on call timestamp
   *   and internal queue state (deemed overloaded).
   */
  private boolean needToDrop(CallRunner callRunner) {
    long now = EnvironmentEdgeManager.currentTime();
    long callDelay = now - callRunner.getRpcCall().getReceiveTime();

    long localMinDelay = this.minDelay;

    // Try and determine if we should reset
    // the delay time and determine overload
    if (now > intervalTime &&
        !resetDelay.get() &&
        !resetDelay.getAndSet(true)) {
      intervalTime = now + codelInterval;

      isOverloaded.set(localMinDelay > codelTargetDelay);
    }

    // If it looks like we should reset the delay
    // time do it only once on one thread
    if (resetDelay.get() && resetDelay.getAndSet(false)) {
      minDelay = callDelay;
      // we just reset the delay dunno about how this will work
      return false;
    } else if (callDelay < localMinDelay) {
      minDelay = callDelay;
    }

    return isOverloaded.get() && callDelay > 2 * codelTargetDelay;
  }

  // Generic BlockingQueue methods we support
  @Override
  public boolean offer(CallRunner callRunner) {
    return queue.offer(callRunner);
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public String toString() {
    return queue.toString();
  }

  // This class does NOT provide generic purpose BlockingQueue implementation,
  // so to prevent misuse all other methods throw UnsupportedOperationException.

  @Override
  public CallRunner poll(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }


  @Override
  public CallRunner peek() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public int drainTo(Collection<? super CallRunner> c) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public int drainTo(Collection<? super CallRunner> c, int maxElements) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public Iterator<CallRunner> iterator() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean add(CallRunner callRunner) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public CallRunner remove() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public CallRunner element() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean addAll(Collection<? extends CallRunner> c) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public void put(CallRunner callRunner) throws InterruptedException {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }

  @Override
  public boolean offer(CallRunner callRunner, long timeout, TimeUnit unit)
      throws InterruptedException {
    throw new UnsupportedOperationException("This class doesn't support anything,"
      + " but take() and offer() methods");
  }
}

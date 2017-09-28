/**
 *
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

import org.apache.yetus.audience.InterfaceAudience;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This queue allows a ThreadPoolExecutor to steal jobs from another ThreadPoolExecutor.
 * This queue also acts as the factory for creating the PriorityBlockingQueue to be used in the
 * steal-from ThreadPoolExecutor. The behavior of this queue is the same as a normal
 * PriorityBlockingQueue except the take/poll(long,TimeUnit) methods would also check whether there
 * are jobs in the steal-from queue if this q ueue is empty.
 *
 * Note the workers in ThreadPoolExecutor must be pre-started so that they can steal job from the
 * other queue, otherwise the worker will only be started after there are jobs submitted to main
 * queue.
 */
@InterfaceAudience.Private
public class StealJobQueue<T> extends PriorityBlockingQueue<T> {

  private static final long serialVersionUID = -6334572230936888291L;

  private BlockingQueue<T> stealFromQueue;

  private final Lock lock = new ReentrantLock();
  private final transient Condition notEmpty = lock.newCondition();

  public StealJobQueue(Comparator<? super T> comparator) {
    this(11, 11, comparator);
  }

  public StealJobQueue(int initCapacity, int stealFromQueueInitCapacity,
      Comparator<? super T> comparator) {
    super(initCapacity, comparator);
    this.stealFromQueue = new PriorityBlockingQueue<T>(stealFromQueueInitCapacity, comparator) {

      private static final long serialVersionUID = -6805567216580184701L;

      @Override
      public boolean offer(T t) {
        lock.lock();
        try {
          notEmpty.signal();
          return super.offer(t);
        } finally {
          lock.unlock();
        }
      }
    };
  }

  /**
   * Get a queue whose job might be stolen by the consumer of this original queue
   * @return the queue whose job could be stolen
   */
  public BlockingQueue<T> getStealFromQueue() {
    return stealFromQueue;
  }

  @Override
  public boolean offer(T t) {
    lock.lock();
    try {
      notEmpty.signal();
      return super.offer(t);
    } finally {
      lock.unlock();
    }
  }


  @Override
  public T take() throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (true) {
        T retVal = this.poll();
        if (retVal == null) {
          retVal = stealFromQueue.poll();
        }
        if (retVal == null) {
          notEmpty.await();
        } else {
          return retVal;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    try {
      while (true) {
        T retVal = this.poll();
        if (retVal == null) {
          retVal = stealFromQueue.poll();
        }
        if (retVal == null) {
          if (nanos <= 0)
            return null;
          nanos = notEmpty.awaitNanos(nanos);
        } else {
          return retVal;
        }
      }
    } finally {
      lock.unlock();
    }
  }
}


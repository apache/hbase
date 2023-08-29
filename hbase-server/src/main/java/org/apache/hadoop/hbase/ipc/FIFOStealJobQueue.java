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
package org.apache.hadoop.hbase.ipc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class FIFOStealJobQueue<T> extends LinkedBlockingQueue<T> {
  private static final long serialVersionUID = -4984005244760265988L;

  private BlockingQueue<T> stealFromQueue;

  private final Lock lock = new ReentrantLock();
  private final transient Condition notEmpty = lock.newCondition();

  public FIFOStealJobQueue(int initCapacity, int stealFromQueueInitCapacity) {
    super(initCapacity);
    this.stealFromQueue = new LinkedBlockingQueue<T>(stealFromQueueInitCapacity) {

      private static final long serialVersionUID = -6059419446245599796L;

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
          if (nanos <= 0) return null;
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

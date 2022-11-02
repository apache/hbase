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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Lock to manage concurrency between RegionScanner and getSmallestReadPoint.
 */
@InterfaceAudience.Private
public class ReadPointCalculationLock {

  public enum LockType {
    CALCULATION_LOCK,
    RECORDING_LOCK
  }

  private final boolean useReadWriteLockForReadPoints;
  private Lock lock;
  private ReadWriteLock readWriteLock;

  ReadPointCalculationLock(Configuration conf) {
    this.useReadWriteLockForReadPoints =
      conf.getBoolean("hbase.readpoints.read.write.lock.enable", false);
    if (useReadWriteLockForReadPoints) {
      readWriteLock = new ReentrantReadWriteLock();
    } else {
      lock = new ReentrantLock();
    }
  }

  void lock(LockType lockType) {
    if (useReadWriteLockForReadPoints) {
      assert lock == null;
      if (lockType == LockType.CALCULATION_LOCK) {
        readWriteLock.writeLock().lock();
      } else {
        readWriteLock.readLock().lock();
      }
    } else {
      assert readWriteLock == null;
      lock.lock();
    }
  }

  void unlock(LockType lockType) {
    if (useReadWriteLockForReadPoints) {
      assert lock == null;
      if (lockType == LockType.CALCULATION_LOCK) {
        readWriteLock.writeLock().unlock();
      } else {
        readWriteLock.readLock().unlock();
      }
    } else {
      assert readWriteLock == null;
      lock.unlock();
    }
  }
}

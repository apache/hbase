package org.apache.hadoop.hbase.util;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@InterfaceAudience.Private
public class IdReadWriteLockStrongRef<T> implements IdReadWriteLock<T> {

  final ConcurrentHashMap<T, ReentrantReadWriteLock> map = new ConcurrentHashMap<>();

  @VisibleForTesting
  @Override
  public ReentrantReadWriteLock getLock(T id) {
    ReentrantReadWriteLock existing = map.get(id);
    if (existing != null) {
      return existing;
    }

    ReentrantReadWriteLock newLock = new ReentrantReadWriteLock();
    existing = map.putIfAbsent(id, newLock);
    if (existing == null) {
      return newLock;
    } else {
      return existing;
    }
  }

  @VisibleForTesting
  @Override
  public void waitForWaiters(T id, int numWaiters) throws InterruptedException {
    for (ReentrantReadWriteLock readWriteLock; ; ) {
      readWriteLock = map.get(id);
      if (readWriteLock != null) {
        synchronized (readWriteLock) {
          if (readWriteLock.getQueueLength() >= numWaiters) {
            return;
          }
        }
      }
      Thread.sleep(50);
    }
  }
}

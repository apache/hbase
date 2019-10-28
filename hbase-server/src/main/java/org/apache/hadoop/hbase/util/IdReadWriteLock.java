package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Allows multiple concurrent clients to lock on a numeric id with ReentrantReadWriteLock. The
 * intended usage for read lock is as follows:
 *
 * <pre>
 * ReentrantReadWriteLock lock = idReadWriteLock.getLock(id);
 * try {
 *   lock.readLock().lock();
 *   // User code.
 * } finally {
 *   lock.readLock().unlock();
 * }
 * </pre>
 *
 * For write lock, use lock.writeLock()
 */
@InterfaceAudience.Private
public interface IdReadWriteLock<T> {
  public ReentrantReadWriteLock getLock(T id);
  public void waitForWaiters(T id, int numWaiters) throws InterruptedException;
}

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

package org.apache.hadoop.hbase.procedure2;

import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Locking for mutual exclusion between procedures. Used only by procedure framework internally.
 * {@link LockAndQueue} has two purposes:
 * <ol>
 *   <li>Acquire/release exclusive/shared locks.</li>
 *   <li>Maintains a list of procedures waiting on this lock.
 *      {@link LockAndQueue} extends {@link ProcedureDeque} class. Blocked Procedures are added
 *      to our super Deque. Using inheritance over composition to keep the Deque of waiting
 *      Procedures is unusual, but we do it this way because in certain cases, there will be
 *      millions of regions. This layout uses less memory.
 * </ol>
 *
 * <p>NOT thread-safe. Needs external concurrency control: e.g. uses in MasterProcedureScheduler are
 * guarded by schedLock().
 * <br>
 * There is no need of 'volatile' keyword for member variables because of memory synchronization
 * guarantees of locks (see 'Memory Synchronization',
 * http://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Lock.html)
 * <br>
 * We do not implement Lock interface because we need exclusive and shared locking, and also
 * because try-lock functions require procedure id.
 * <br>
 * We do not use ReentrantReadWriteLock directly because of its high memory overhead.
 */
@InterfaceAudience.Private
public class LockAndQueue implements LockStatus {
  private final ProcedureDeque queue = new ProcedureDeque();
  private Procedure<?> exclusiveLockOwnerProcedure = null;
  private int sharedLock = 0;

  // ======================================================================
  //  Lock Status
  // ======================================================================

  @Override
  public boolean isLocked() {
    return hasExclusiveLock() || sharedLock > 0;
  }

  @Override
  public boolean hasExclusiveLock() {
    return this.exclusiveLockOwnerProcedure != null;
  }

  @Override
  public boolean isLockOwner(long procId) {
    return getExclusiveLockProcIdOwner() == procId;
  }

  @Override
  public boolean hasParentLock(Procedure<?> proc) {
    // TODO: need to check all the ancestors. need to passed in the procedures
    // to find the ancestors.
    return proc.hasParent() &&
      (isLockOwner(proc.getParentProcId()) || isLockOwner(proc.getRootProcId()));
  }

  @Override
  public boolean hasLockAccess(Procedure<?> proc) {
    return isLockOwner(proc.getProcId()) || hasParentLock(proc);
  }

  @Override
  public Procedure<?> getExclusiveLockOwnerProcedure() {
    return exclusiveLockOwnerProcedure;
  }

  @Override
  public long getExclusiveLockProcIdOwner() {
    if (exclusiveLockOwnerProcedure == null) {
      return Long.MIN_VALUE;
    } else {
      return exclusiveLockOwnerProcedure.getProcId();
    }
  }

  @Override
  public int getSharedLockCount() {
    return sharedLock;
  }

  // ======================================================================
  //  try/release Shared/Exclusive lock
  // ======================================================================

  /**
   * @return whether we have succesfully acquired the shared lock.
   */
  public boolean trySharedLock(Procedure<?> proc) {
    if (hasExclusiveLock() && !hasLockAccess(proc)) {
      return false;
    }
    // If no one holds the xlock, then we are free to hold the sharedLock
    // If the parent proc or we have already held the xlock, then we return true here as
    // xlock is more powerful then shared lock.
    sharedLock++;
    return true;
  }

  /**
   * @return whether we should wake the procedures waiting on the lock here.
   */
  public boolean releaseSharedLock() {
    // hasExclusiveLock could be true, it usually means we acquire shared lock while we or our
    // parent have held the xlock. And since there is still an exclusive lock, we do not need to
    // wake any procedures.
    return --sharedLock == 0 && !hasExclusiveLock();
  }

  public boolean tryExclusiveLock(Procedure<?> proc) {
    if (isLocked()) {
      return hasLockAccess(proc);
    }
    exclusiveLockOwnerProcedure = proc;
    return true;
  }

  /**
   * @return whether we should wake the procedures waiting on the lock here.
   */
  public boolean releaseExclusiveLock(Procedure<?> proc) {
    if (!isLockOwner(proc.getProcId())) {
      // We are not the lock owner, it is probably inherited from the parent procedures.
      return false;
    }
    exclusiveLockOwnerProcedure = null;
    // This maybe a bit strange so let me explain. We allow acquiring shared lock while the parent
    // proc or we have already held the xlock, and also allow releasing the locks in any order, so
    // it could happen that the xlock is released but there are still some procs holding the shared
    // lock.
    // In HBase, this could happen when a proc which holdLock is false and schedules sub procs which
    // acquire the shared lock on the same lock. This is because we will schedule the sub proces
    // before releasing the lock, so the sub procs could call acquire lock before we releasing the
    // xlock.
    return sharedLock == 0;
  }

  public boolean isWaitingQueueEmpty() {
    return queue.isEmpty();
  }

  public Procedure<?> removeFirst() {
    return queue.removeFirst();
  }

  public void addLast(Procedure<?> proc) {
    queue.addLast(proc);
  }

  public int wakeWaitingProcedures(ProcedureScheduler scheduler) {
    int count = queue.size();
    // wakeProcedure adds to the front of queue, so we start from last in the waitQueue' queue, so
    // that the procedure which was added first goes in the front for the scheduler queue.
    scheduler.addFront(queue.descendingIterator());
    queue.clear();
    return count;
  }

  @SuppressWarnings("rawtypes")
  public Stream<Procedure> filterWaitingQueue(Predicate<Procedure> predicate) {
    return queue.stream().filter(predicate);
  }

  @Override
  public String toString() {
    return "exclusiveLockOwner=" + (hasExclusiveLock() ? getExclusiveLockProcIdOwner() : "NONE") +
      ", sharedLockCount=" + getSharedLockCount() + ", waitingProcCount=" + queue.size();
  }
}

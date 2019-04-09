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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface to get status of a Lock without getting access to acquire/release lock. Currently used
 * in MasterProcedureScheduler where we want to give Queues access to lock's status for scheduling
 * purposes, but not the ability to acquire/release it.
 */
@InterfaceAudience.Private
public interface LockStatus {

  /**
   * Return whether this lock has already been held,
   * <p/>
   * Notice that, holding the exclusive lock or shared lock are both considered as locked, i.e, this
   * method usually equals to {@code hasExclusiveLock() || getSharedLockCount() > 0}.
   */
  default boolean isLocked() {
    return hasExclusiveLock() || getSharedLockCount() > 0;
  }

  /**
   * Whether the exclusive lock has been held.
   */
  boolean hasExclusiveLock();

  /**
   * Return true if the procedure itself holds the exclusive lock, or any ancestors of the give
   * procedure hold the exclusive lock.
   */
  boolean hasLockAccess(Procedure<?> proc);

  /**
   * Get the procedure which holds the exclusive lock.
   */
  Procedure<?> getExclusiveLockOwnerProcedure();

  /**
   * Return the id of the procedure which holds the exclusive lock, if exists. Or a negative value
   * which means no one holds the exclusive lock.
   * <p/>
   * Notice that, in HBase, we assume that the procedure id is positive, or at least non-negative.
   */
  default long getExclusiveLockProcIdOwner() {
    Procedure<?> proc = getExclusiveLockOwnerProcedure();
    return proc != null ? proc.getProcId() : -1L;
  }

  /**
   * Get the number of procedures which hold the shared lock.
   */
  int getSharedLockCount();
}

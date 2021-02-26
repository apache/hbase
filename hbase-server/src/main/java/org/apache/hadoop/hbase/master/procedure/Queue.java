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
package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.procedure2.LockStatus;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureDeque;
import org.apache.hadoop.hbase.util.AvlUtil.AvlLinkedNode;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
abstract class Queue<TKey extends Comparable<TKey>> extends AvlLinkedNode<Queue<TKey>> {

  /**
   * @param proc must not be null
   */
  abstract boolean requireExclusiveLock(Procedure<?> proc);

  private final TKey key;
  private final int priority;
  private final ProcedureDeque runnables = new ProcedureDeque();
  // Reference to status of lock on entity this queue represents.
  private final LockStatus lockStatus;

  protected Queue(TKey key, LockStatus lockStatus) {
    this(key, 1, lockStatus);
  }

  protected Queue(TKey key, int priority, LockStatus lockStatus) {
    assert priority >= 1 : "priority must be greater than or equal to 1";
    this.key = key;
    this.priority = priority;
    this.lockStatus = lockStatus;
  }

  protected TKey getKey() {
    return key;
  }

  public int getPriority() {
    return priority;
  }

  protected LockStatus getLockStatus() {
    return lockStatus;
  }

  public boolean isAvailable() {
    return !isEmpty();
  }

  // ======================================================================
  // Functions to handle procedure queue
  // ======================================================================
  public void add(Procedure<?> proc, boolean addToFront) {
    if (addToFront) {
      runnables.addFirst(proc);
    } else {
      runnables.addLast(proc);
    }
  }

  public Procedure<?> peek() {
    return runnables.peek();
  }

  public Procedure<?> poll() {
    return runnables.poll();
  }

  public boolean isEmpty() {
    return runnables.isEmpty();
  }

  public int size() {
    return runnables.size();
  }

  // ======================================================================
  // Generic Helpers
  // ======================================================================
  public int compareKey(TKey cmpKey) {
    return key.compareTo(cmpKey);
  }

  @Override
  public int compareTo(Queue<TKey> other) {
    return compareKey(other.key);
  }

  @Override
  public String toString() {
    return String.format("%s(%s, xlock=%s sharedLock=%s size=%s)", getClass().getSimpleName(), key,
      lockStatus.hasExclusiveLock() ? "true (" + lockStatus.getExclusiveLockProcIdOwner() + ")"
          : "false",
      lockStatus.getSharedLockCount(), size());
  }
}

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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A simple barrier that can be used by classes that need to wait for some operations to
 * finish before stopping/closing/etc. forever.
 */
@InterfaceAudience.Private
public class DrainBarrier {
  /**
   * Contains the number of outstanding operations, as well as flags.
   * Initially, the number of operations is 1. Each beginOp increments, and endOp decrements it.
   * beginOp does not proceed when it sees the draining flag. When stop is called, it atomically
   * decrements the number of operations (the initial 1) and sets the draining flag. If stop did
   * the decrement to zero, that means there are no more operations outstanding, so stop is done.
   * Otherwise, stop blocks, and the endOp that decrements the count to 0 unblocks it.
   */
  private final AtomicLong valueAndFlags = new AtomicLong(inc(0));
  private final static long DRAINING_FLAG = 0x1;
  private final static int FLAG_BIT_COUNT = 1;

  /**
   * Tries to start an operation.
   * @return false iff the stop is in progress, and the operation cannot be started.
   */
  public boolean beginOp() {
    long oldValAndFlags;
    do {
      oldValAndFlags = valueAndFlags.get();
      if (isDraining(oldValAndFlags)) return false;
    } while (!valueAndFlags.compareAndSet(oldValAndFlags, inc(oldValAndFlags)));
    return true;
  }

  /**
   * Ends the operation. Unblocks the blocked caller of stop, if necessary.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
      justification="First, we do change the state before notify, 2nd, it doesn't even matter")
  public void endOp() {
    long oldValAndFlags;
    do {
      oldValAndFlags = valueAndFlags.get();
      long unacceptableCount = isDraining(oldValAndFlags) ? 0 : 1;
      if (getValue(oldValAndFlags) == unacceptableCount) {
        throw new AssertionError("endOp called without corresponding beginOp call ("
          + "the current count is " + unacceptableCount + ")");
      }
    } while (!valueAndFlags.compareAndSet(oldValAndFlags, dec(oldValAndFlags)));
    if (getValue(oldValAndFlags) == 1) {
      synchronized (this) { this.notifyAll(); }
    }
  }

  /**
   * Blocks new operations from starting, waits for the current ones to drain.
   * If someone already called it, returns immediately, which is currently unavoidable as
   * most of the users stop and close things right and left, and hope for the best.
   * stopAndWaitForOpsOnce asserts instead.
   * @throws InterruptedException the wait for operations has been interrupted.
   */
  public void stopAndDrainOps() throws InterruptedException {
    stopAndDrainOps(true);
  }

  /**
   * Blocks new operations from starting, waits for the current ones to drain.
   * Can only be called once.
   * @throws InterruptedException the wait for operations has been interrupted.
   */
  public void stopAndDrainOpsOnce() throws InterruptedException {
    stopAndDrainOps(false);
  }

  /**
   * @param ignoreRepeatedCalls If this is true and somebody already called stop, this method
   *                            will return immediately if true; if this is false and somebody
   *                            already called stop, it will assert.
   */
  // Justification for warnings - wait is not unconditional, and contrary to what WA_NOT_IN_LOOP
  // description says we are not waiting on multiple conditions.
  @edu.umd.cs.findbugs.annotations.SuppressWarnings({"UW_UNCOND_WAIT", "WA_NOT_IN_LOOP"})
  private void stopAndDrainOps(boolean ignoreRepeatedCalls) throws InterruptedException {
    long oldValAndFlags;
    do {
      oldValAndFlags = valueAndFlags.get();
      if (isDraining(oldValAndFlags)) {
        if (ignoreRepeatedCalls) return;
        throw new AssertionError("stopAndWaitForOpsOnce called more than once");
      }
    } while (!valueAndFlags.compareAndSet(oldValAndFlags, dec(oldValAndFlags) | DRAINING_FLAG));
    if (getValue(oldValAndFlags) == 1) return; // There were no operations outstanding.
    synchronized (this) { this.wait(); }
  }

  // Helper methods.
  private static final boolean isDraining(long valueAndFlags) {
    return (valueAndFlags & DRAINING_FLAG) == DRAINING_FLAG;
  }

  private static final long getValue(long valueAndFlags) {
    return valueAndFlags >> FLAG_BIT_COUNT;
  }

  private static final long inc(long valueAndFlags) {
    return valueAndFlags + (1 << FLAG_BIT_COUNT); // Not checking for overflow.
  }

  private static final long dec(long valueAndFlags) {
    return valueAndFlags - (1 << FLAG_BIT_COUNT); // Negative overflow checked outside.
  }
}

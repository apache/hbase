/*
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

package org.apache.hadoop.hbase.client;

import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * An interface for client request scheduling algorithm.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RequestController {

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum ReturnCode {
    /**
     * Accept current row.
     */
    INCLUDE,
    /**
     * Skip current row.
     */
    SKIP,
    /**
     * No more row can be included.
     */
    END
  }

  /**
   * Picks up the valid data.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public interface Checker {
    /**
     * Checks the data whether it is valid to submit.
     * @param loc the destination of data
     * @param row the data to check
     * @return describe the decision for the row
     */
    ReturnCode canTakeRow(HRegionLocation loc, Row row);

    /**
     * Reset the state of the scheduler when completing the iteration of rows.
     * @throws InterruptedIOException some controller may wait
     * for some busy region or RS to complete the undealt request.
     */
    void reset() throws InterruptedIOException ;
  }

  /**
   * @return A new checker for evaluating a batch rows.
   */
  Checker newChecker();

  /**
   * Increment the counter if we build a valid task.
   * @param regions The destination of task
   * @param sn The target server
   */
  void incTaskCounters(Collection<byte[]> regions, ServerName sn);

  /**
   * Decrement the counter if a task is accomplished.
   * @param regions The destination of task
   * @param sn The target server
   */
  void decTaskCounters(Collection<byte[]> regions, ServerName sn);

  /**
   * @return The number of running task.
   */
  long getNumberOfTasksInProgress();

  /**
   * Waits for the running tasks to complete.
   * If there are specified threshold and trigger, the implementation should
   * wake up once in a while for checking the threshold and calling trigger.
   * @param max This method will return if the number of running tasks is
   * less than or equal to max.
   * @param id the caller's id
   * @param periodToTrigger The period to invoke the trigger. This value is a
   * hint. The real period depends on the implementation.
   * @param trigger The object to call periodically.
   * @throws java.io.InterruptedIOException If the waiting is interrupted
   */
  void waitForMaximumCurrentTasks(long max, long id,
    int periodToTrigger, Consumer<Long> trigger) throws InterruptedIOException;

  /**
   * Wait until there is at least one slot for a new task.
   * @param id the caller's id
   * @param periodToTrigger The period to invoke the trigger. This value is a
   * hint. The real period depends on the implementation.
   * @param trigger The object to call periodically.
   * @throws java.io.InterruptedIOException If the waiting is interrupted
   */
  void waitForFreeSlot(long id, int periodToTrigger,
          Consumer<Long> trigger) throws InterruptedIOException;
}

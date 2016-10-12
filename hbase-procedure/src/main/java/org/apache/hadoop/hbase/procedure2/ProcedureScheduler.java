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

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Keep track of the runnable procedures
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ProcedureScheduler {
  /**
   * Start the scheduler
   */
  void start();

  /**
   * Stop the scheduler
   */
  void stop();

  /**
   * In case the class is blocking on poll() waiting for items to be added,
   * this method should awake poll() and poll() should return.
   */
  void signalAll();

  /**
   * Inserts the specified element at the front of this queue.
   * @param proc the Procedure to add
   */
  void addFront(Procedure proc);

  /**
   * Inserts the specified element at the end of this queue.
   * @param proc the Procedure to add
   */
  void addBack(Procedure proc);

  /**
   * The procedure can't run at the moment.
   * add it back to the queue, giving priority to someone else.
   * @param proc the Procedure to add back to the list
   */
  void yield(Procedure proc);

  /**
   * The procedure in execution completed.
   * This can be implemented to perform cleanups.
   * @param proc the Procedure that completed the execution.
   */
  void completionCleanup(Procedure proc);

  /**
   * @return true if there are procedures available to process, otherwise false.
   */
  boolean hasRunnables();

  /**
   * Fetch one Procedure from the queue
   * @return the Procedure to execute, or null if nothing present.
   */
  Procedure poll();

  /**
   * Fetch one Procedure from the queue
   * @param timeout how long to wait before giving up, in units of unit
   * @param unit a TimeUnit determining how to interpret the timeout parameter
   * @return the Procedure to execute, or null if nothing present.
   */
  Procedure poll(long timeout, TimeUnit unit);

  /**
   * Mark the event has not ready.
   * procedures calling waitEvent() will be suspended.
   * @param event the event to mark as suspended/not ready
   */
  void suspendEvent(ProcedureEvent event);

  /**
   * Wake every procedure waiting for the specified event
   * (By design each event has only one "wake" caller)
   * @param event the event to wait
   */
  void wakeEvent(ProcedureEvent event);

  /**
   * Wake every procedure waiting for the specified events.
   * (By design each event has only one "wake" caller)
   * @param count the number of events in the array to wake
   * @param events the list of events to wake
   */
  void wakeEvents(int count, ProcedureEvent... events);

  /**
   * Suspend the procedure if the event is not ready yet.
   * @param event the event to wait on
   * @param procedure the procedure waiting on the event
   * @return true if the procedure has to wait for the event to be ready, false otherwise.
   */
  boolean waitEvent(ProcedureEvent event, Procedure procedure);

  /**
   * Returns the number of elements in this queue.
   * @return the number of elements in this queue.
   */
  @VisibleForTesting
  int size();

  /**
   * Removes all of the elements from the queue
   */
  @VisibleForTesting
  void clear();
}

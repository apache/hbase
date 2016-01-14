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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Keep track of the runnable procedures
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ProcedureRunnableSet {
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
   * Fetch one Procedure from the queue
   * @return the Procedure to execute, or null if nothing present.
   */
  Procedure poll();

  /**
   * In case the class is blocking on poll() waiting for items to be added,
   * this method should awake poll() and poll() should return.
   */
  void signalAll();

  /**
   * Returns the number of elements in this collection.
   * @return the number of elements in this collection.
   */
  int size();

  /**
   * Removes all of the elements from this collection.
   */
  void clear();
}

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

package org.apache.hadoop.hbase.procedure2.store;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.procedure2.Procedure;

/**
 * The ProcedureStore is used by the executor to persist the state of each procedure execution.
 * This allows to resume the execution of pending/in-progress procedures in case
 * of machine failure or service shutdown.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ProcedureStore {
  /**
   * Store listener interface.
   * The main process should register a listener and respond to the store events.
   */
  public interface ProcedureStoreListener {
    /**
     * triggered when the store sync is completed.
     */
    void postSync();

    /**
     * triggered when the store is not able to write out data.
     * the main process should abort.
     */
    void abortProcess();
  }

  /**
   * Add the listener to the notification list.
   * @param listener The AssignmentListener to register
   */
  void registerListener(ProcedureStoreListener listener);

  /**
   * Remove the listener from the notification list.
   * @param listener The AssignmentListener to unregister
   * @return true if the listner was in the list and it was removed, otherwise false.
   */
  boolean unregisterListener(ProcedureStoreListener listener);

  /**
   * Start/Open the procedure store
   * @param numThreads
   */
  void start(int numThreads) throws IOException;

  /**
   * Stop/Close the procedure store
   * @param abort true if the stop is an abort
   */
  void stop(boolean abort);

  /**
   * @return true if the store is running, otherwise false.
   */
  boolean isRunning();

  /**
   * @return the number of threads/slots passed to start()
   */
  int getNumThreads();

  /**
   * Acquire the lease for the procedure store.
   */
  void recoverLease() throws IOException;

  /**
   * Load the Procedures in the store.
   * @return the set of procedures present in the store
   */
  Iterator<Procedure> load() throws IOException;

  /**
   * When a procedure is submitted to the executor insert(proc, null) will be called.
   * 'proc' has a 'RUNNABLE' state and the initial information required to start up.
   *
   * When a procedure is executed and it returns children insert(proc, subprocs) will be called.
   * 'proc' has a 'WAITING' state and an update state.
   * 'subprocs' are the children in 'RUNNABLE' state with the initial information.
   *
   * @param proc the procedure to serialize and write to the store.
   * @param subprocs the newly created child of the proc.
   */
  void insert(Procedure proc, Procedure[] subprocs);

  /**
   * The specified procedure was executed,
   * and the new state should be written to the store.
   * @param proc the procedure to serialize and write to the store.
   */
  void update(Procedure proc);

  /**
   * The specified procId was removed from the executor,
   * due to completion, abort or failure.
   * The store implementor should remove all the information about the specified procId.
   * @param procId the ID of the procedure to remove.
   */
  void delete(long procId);
}
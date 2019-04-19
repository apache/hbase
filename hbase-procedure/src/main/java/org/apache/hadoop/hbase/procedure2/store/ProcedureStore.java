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

import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

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
   * <p/>
   * The main process should register a listener and respond to the store events.
   */
  public interface ProcedureStoreListener {

    /**
     * triggered when the store sync is completed.
     */
    default void postSync() {
    }

    /**
     * triggered when the store is not able to write out data. the main process should abort.
     */
    default void abortProcess() {
    }

    /**
     * Suggest that the upper layer should update the state of some procedures. Ignore this call
     * will not effect correctness but performance.
     * <p/>
     * For a WAL based ProcedureStore implementation, if all the procedures stored in a WAL file
     * have been deleted, or updated later in another WAL file, then we can delete the WAL file. If
     * there are old procedures in a WAL file which are never deleted or updated, then we can not
     * delete the WAL file and this will cause we hold lots of WAL file and slow down the master
     * restarts. So here we introduce this method to tell the upper layer that please update the
     * states of these procedures so that we can delete the old WAL file.
     * @param procIds the id for the procedures
     */
    default void forceUpdate(long[] procIds) {
    }
  }

  /**
   * An Iterator over a collection of Procedure
   */
  public interface ProcedureIterator {
    /**
     * Reset the Iterator by seeking to the beginning of the list.
     */
    void reset();

    /**
     * Returns true if the iterator has more elements.
     * (In other words, returns true if next() would return a Procedure
     * rather than throwing an exception.)
     * @return true if the iterator has more procedures
     */
    boolean hasNext();

    /**
     * Calling this method does not need to convert the protobuf message to the Procedure class, so
     * if it returns true we can call {@link #skipNext()} to skip the procedure without
     * deserializing. This could increase the performance.
     * @return true if the iterator next element is a completed procedure.
     */
    boolean isNextFinished();

    /**
     * Skip the next procedure
     * <p/>
     * This method is used to skip the deserializing of the procedure to increase performance, as
     * when calling next we need to convert the protobuf message to the Procedure class.
     */
    void skipNext();

    /**
     * Returns the next procedure in the iteration.
     * @throws IOException if there was an error fetching/deserializing the procedure
     * @return the next procedure in the iteration.
     */
    @SuppressWarnings("rawtypes")
    Procedure next() throws IOException;
  }

  /**
   * Interface passed to the ProcedureStore.load() method to handle the store-load events.
   */
  public interface ProcedureLoader {
    /**
     * Called by ProcedureStore.load() to notify about the maximum proc-id in the store.
     * @param maxProcId the highest proc-id in the store
     */
    void setMaxProcId(long maxProcId);

    /**
     * Called by the ProcedureStore.load() every time a set of procedures are ready to be executed.
     * The ProcedureIterator passed to the method, has the procedure sorted in replay-order.
     * @param procIter iterator over the procedures ready to be added to the executor.
     */
    void load(ProcedureIterator procIter) throws IOException;

    /**
     * Called by the ProcedureStore.load() in case we have procedures not-ready to be added to
     * the executor, which probably means they are corrupted since some information/link is missing.
     * @param procIter iterator over the procedures not ready to be added to the executor, corrupted
     */
    void handleCorrupted(ProcedureIterator procIter) throws IOException;
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
   * @param numThreads number of threads to be used by the procedure store
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
   * Set the number of procedure running.
   * This can be used, for example, by the store to know how long to wait before a sync.
   * @return how many procedures are running (may not be same as <code>count</code>).
   */
  int setRunningProcedureCount(int count);

  /**
   * Acquire the lease for the procedure store.
   */
  void recoverLease() throws IOException;

  /**
   * Load the Procedures in the store.
   * @param loader the ProcedureLoader that will handle the store-load events
   */
  void load(ProcedureLoader loader) throws IOException;

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
  void insert(Procedure<?> proc, Procedure<?>[] subprocs);

  /**
   * Serialize a set of new procedures.
   * These procedures are freshly submitted to the executor and each procedure
   * has a 'RUNNABLE' state and the initial information required to start up.
   *
   * @param procs the procedures to serialize and write to the store.
   */
  void insert(Procedure<?>[] procs);

  /**
   * The specified procedure was executed,
   * and the new state should be written to the store.
   * @param proc the procedure to serialize and write to the store.
   */
  void update(Procedure<?> proc);

  /**
   * The specified procId was removed from the executor,
   * due to completion, abort or failure.
   * The store implementor should remove all the information about the specified procId.
   * @param procId the ID of the procedure to remove.
   */
  void delete(long procId);

  /**
   * The parent procedure completed.
   * Update the state and mark all the child deleted.
   * @param parentProc the parent procedure to serialize and write to the store.
   * @param subProcIds the IDs of the sub-procedure to remove.
   */
  void delete(Procedure<?> parentProc, long[] subProcIds);

  /**
   * The specified procIds were removed from the executor,
   * due to completion, abort or failure.
   * The store implementor should remove all the information about the specified procIds.
   * @param procIds the IDs of the procedures to remove.
   * @param offset the array offset from where to start to delete
   * @param count the number of IDs to delete
   */
  void delete(long[] procIds, int offset, int count);
}

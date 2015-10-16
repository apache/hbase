/*
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.PairOfSameType;

/**
 * Executes region split as a "transaction".  Call {@link #prepare()} to setup
 * the transaction, {@link #execute(Server, RegionServerServices)} to run the
 * transaction and {@link #rollback(Server, RegionServerServices)} to cleanup if execute fails.
 *
 * <p>Here is an example of how you would use this interface:
 * <pre>
 *  SplitTransactionFactory factory = new SplitTransactionFactory(conf);
 *  SplitTransaction st = factory.create(parent, midKey)
 *    .registerTransactionListener(new TransactionListener() {
 *       public void transition(SplitTransaction transaction, SplitTransactionPhase from,
 *           SplitTransactionPhase to) throws IOException {
 *         // ...
 *       }
 *       public void rollback(SplitTransaction transaction, SplitTransactionPhase from,
 *           SplitTransactionPhase to) {
 *         // ...
 *       }
 *    });
 *  if (!st.prepare()) return;
 *  try {
 *    st.execute(server, services);
 *  } catch (IOException e) {
 *    try {
 *      st.rollback(server, services);
 *      return;
 *    } catch (RuntimeException e) {
 *      // abort the server
 *    }
 *  }
 * </Pre>
 * <p>A split transaction is not thread safe.  Callers must ensure a split is run by
 * one thread only.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface SplitTransaction {

  /**
   * Each enum is a step in the split transaction.
   */
  public enum SplitTransactionPhase {
    /**
     * Started
     */
    STARTED,
    /**
     * Prepared
     */
    PREPARED,
    /**
     * Before preSplit coprocessor hook
     */
    BEFORE_PRE_SPLIT_HOOK,
    /**
     * After preSplit coprocessor hook
     */
    AFTER_PRE_SPLIT_HOOK,
    /**
     * Set region as in transition, set it into SPLITTING state.
     */
    SET_SPLITTING,
    /**
     * We created the temporary split data directory.
     */
    CREATE_SPLIT_DIR,
    /**
     * Closed the parent region.
     */
    CLOSED_PARENT_REGION,
    /**
     * The parent has been taken out of the server's online regions list.
     */
    OFFLINED_PARENT,
    /**
     * Started in on creation of the first daughter region.
     */
    STARTED_REGION_A_CREATION,
    /**
     * Started in on the creation of the second daughter region.
     */
    STARTED_REGION_B_CREATION,
    /**
     * Opened the first daughter region
     */
    OPENED_REGION_A,
    /**
     * Opened the second daughter region
     */
    OPENED_REGION_B,
    /**
     * Point of no return.
     * If we got here, then transaction is not recoverable other than by
     * crashing out the regionserver.
     */
    PONR,
    /**
     * Before postSplit coprocessor hook
     */
    BEFORE_POST_SPLIT_HOOK,
    /**
     * After postSplit coprocessor hook
     */
    AFTER_POST_SPLIT_HOOK,
    /**
     * Completed
     */
    COMPLETED
  }

  /**
   * Split transaction journal entry
   */
  public interface JournalEntry {

    /** @return the completed phase marked by this journal entry */
    SplitTransactionPhase getPhase();

    /** @return the time of phase completion */
    long getTimeStamp();
  }

  /**
   * Split transaction listener
   */
  public interface TransactionListener {

    /**
     * Invoked when transitioning forward from one transaction phase to another
     * @param transaction the transaction
     * @param from the current phase
     * @param to the next phase
     * @throws IOException listener can throw this to abort
     */
    void transition(SplitTransaction transaction, SplitTransactionPhase from,
        SplitTransactionPhase to) throws IOException;

    /**
     * Invoked when rolling back a transaction from one transaction phase to the
     * previous
     * @param transaction the transaction
     * @param from the current phase
     * @param to the previous phase
     */
    void rollback(SplitTransaction transaction, SplitTransactionPhase from,
        SplitTransactionPhase to);
  }

  /**
   * Check split inputs and prepare the transaction.
   * @return <code>true</code> if the region is splittable else
   * <code>false</code> if it is not (e.g. its already closed, etc.).
   * @throws IOException 
   */
  boolean prepare() throws IOException;

  /**
   * Run the transaction.
   * @param server Hosting server instance.  Can be null when testing.
   * @param services Used to online/offline regions.
   * @throws IOException If thrown, transaction failed.
   *          Call {@link #rollback(Server, RegionServerServices)}
   * @return Regions created
   * @throws IOException
   * @see #rollback(Server, RegionServerServices)
   * @deprecated use #execute(Server, RegionServerServices, User)
   */
  @Deprecated
  PairOfSameType<Region> execute(Server server, RegionServerServices services) throws IOException;

  /**
   * Run the transaction.
   * @param server Hosting server instance.  Can be null when testing.
   * @param services Used to online/offline regions.
   * @param user
   * @throws IOException If thrown, transaction failed.
   *          Call {@link #rollback(Server, RegionServerServices)}
   * @return Regions created
   * @throws IOException
   * @see #rollback(Server, RegionServerServices)
   */
  PairOfSameType<Region> execute(Server server, RegionServerServices services, User user)
      throws IOException;

  /**
   * Roll back a failed transaction
   * @param server Hosting server instance (May be null when testing).
   * @param services
   * @throws IOException If thrown, rollback failed.  Take drastic action.
   * @return True if we successfully rolled back, false if we got to the point
   * of no return and so now need to abort the server to minimize damage.
   * @deprecated use #rollback(Server, RegionServerServices, User)
   */
  @Deprecated
  boolean rollback(Server server, RegionServerServices services) throws IOException;

  /**
   * Roll back a failed transaction
   * @param server Hosting server instance (May be null when testing).
   * @param services
   * @param user
   * @throws IOException If thrown, rollback failed.  Take drastic action.
   * @return True if we successfully rolled back, false if we got to the point
   * of no return and so now need to abort the server to minimize damage.
   */
  boolean rollback(Server server, RegionServerServices services, User user) throws IOException;

  /**
   * Register a listener for transaction preparation, execution, and possibly
   * rollback phases.
   * <p>A listener can abort a transaction by throwing an exception. 
   * @param listener the listener
   * @return 'this' for chaining
   */
  SplitTransaction registerTransactionListener(TransactionListener listener);

  /**
   * Get the journal for the transaction.
   * <p>Journal entries are an opaque type represented as JournalEntry. They can
   * also provide useful debugging information via their toString method.
   * @return the transaction journal
   */
  List<JournalEntry> getJournal();

  /**
   * Get the Server running the transaction or rollback
   * @return server instance
   */
  Server getServer();

  /**
   * Get the RegonServerServices of the server running the transaction or rollback
   * @return region server services
   */
  RegionServerServices getRegionServerServices();
}

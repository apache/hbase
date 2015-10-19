/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.security.User;

/**
 * Executes region merge as a "transaction". It is similar with
 * SplitTransaction. Call {@link #prepare(RegionServerServices)} to setup the
 * transaction, {@link #execute(Server, RegionServerServices)} to run the
 * transaction and {@link #rollback(Server, RegionServerServices)} to cleanup if
 * execute fails.
 * 
 * <p>Here is an example of how you would use this interface:
 * <pre>
 *  RegionMergeTransactionFactory factory = new RegionMergeTransactionFactory(conf);
 *  RegionMergeTransaction mt = factory.create(parent, midKey)
 *    .registerTransactionListener(new TransactionListener() {
 *       public void transition(RegionMergeTransaction transaction,
 *         RegionMergeTransactionPhase from, RegionMergeTransactionPhase to) throws IOException {
 *         // ...
 *       }
 *       public void rollback(RegionMergeTransaction transaction,
 *         RegionMergeTransactionPhase from, RegionMergeTransactionPhase to) {
 *         // ...
 *       }
 *    });
 *  if (!mt.prepare()) return;
 *  try {
 *    mt.execute(server, services);
 *  } catch (IOException ioe) {
 *    try {
 *      mt.rollback(server, services);
 *      return;
 *    } catch (RuntimeException e) {
 *      // abort the server
 *    }
 *  }
 * </Pre>
 * <p>A merge transaction is not thread safe.  Callers must ensure a split is run by
 * one thread only.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RegionMergeTransaction {
  /**
   * Each enum is a step in the merge transaction.
   */
  enum RegionMergeTransactionPhase {
    STARTED,
    /**
     * Prepared
     */
    PREPARED,
    /**
     * Set region as in transition, set it into MERGING state.
     */
    SET_MERGING,
    /**
     * We created the temporary merge data directory.
     */
    CREATED_MERGE_DIR,
    /**
     * Closed the merging region A.
     */
    CLOSED_REGION_A,
    /**
     * The merging region A has been taken out of the server's online regions list.
     */
    OFFLINED_REGION_A,
    /**
     * Closed the merging region B.
     */
    CLOSED_REGION_B,
    /**
     * The merging region B has been taken out of the server's online regions list.
     */
    OFFLINED_REGION_B,
    /**
     * Started in on creation of the merged region.
     */
    STARTED_MERGED_REGION_CREATION,
    /**
     * Point of no return. If we got here, then transaction is not recoverable
     * other than by crashing out the regionserver.
     */
    PONR,
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
    RegionMergeTransactionPhase getPhase();

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
    void transition(RegionMergeTransaction transaction, RegionMergeTransactionPhase from,
        RegionMergeTransactionPhase to) throws IOException;

    /**
     * Invoked when rolling back a transaction from one transaction phase to the
     * previous
     * @param transaction the transaction
     * @param from the current phase
     * @param to the previous phase
     */
    void rollback(RegionMergeTransaction transaction, RegionMergeTransactionPhase from,
        RegionMergeTransactionPhase to);
  }

  /**
   * Check merge inputs and prepare the transaction.
   * @param services
   * @return <code>true</code> if the regions are mergeable else
   *         <code>false</code> if they are not (e.g. its already closed, etc.).
   * @throws IOException 
   */
  boolean prepare(RegionServerServices services) throws IOException;

  /**
   * Run the transaction.
   * @param server Hosting server instance. Can be null when testing
   * @param services Used to online/offline regions.
   * @throws IOException If thrown, transaction failed. Call
   *           {@link #rollback(Server, RegionServerServices)}
   * @return merged region
   * @throws IOException
   * @see #rollback(Server, RegionServerServices)
   * @deprecated use #execute(Server, RegionServerServices, User)
   */
  @Deprecated
  Region execute(Server server, RegionServerServices services) throws IOException;

  /**
   * Run the transaction.
   * @param server Hosting server instance. Can be null when testing
   * @param services Used to online/offline regions.
   * @param user
   * @throws IOException If thrown, transaction failed. Call
   *           {@link #rollback(Server, RegionServerServices)}
   * @return merged region
   * @throws IOException
   * @see #rollback(Server, RegionServerServices, User)
   */
  Region execute(Server server, RegionServerServices services, User user) throws IOException;

  /**
   * Roll back a failed transaction
   * @param server Hosting server instance (May be null when testing).
   * @param services Services of regionserver, used to online regions.
   * @throws IOException If thrown, rollback failed. Take drastic action.
   * @return True if we successfully rolled back, false if we got to the point
   *         of no return and so now need to abort the server to minimize
   *         damage.
   * @deprecated use #rollback(Server, RegionServerServices, User)
   */
  @Deprecated
  boolean rollback(Server server, RegionServerServices services) throws IOException;

  /**
   * Roll back a failed transaction
   * @param server Hosting server instance (May be null when testing).
   * @param services Services of regionserver, used to online regions.
   * @param user
   * @throws IOException If thrown, rollback failed. Take drastic action.
   * @return True if we successfully rolled back, false if we got to the point
   *         of no return and so now need to abort the server to minimize
   *         damage.
   */
  boolean rollback(Server server, RegionServerServices services, User user) throws IOException;

  /**
   * Register a listener for transaction preparation, execution, and possibly
   * rollback phases.
   * <p>A listener can abort a transaction by throwing an exception. 
   * @param listener the listener
   * @return 'this' for chaining
   */
  RegionMergeTransaction registerTransactionListener(TransactionListener listener);

  /** @return merged region info */
  HRegionInfo getMergedRegionInfo();

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

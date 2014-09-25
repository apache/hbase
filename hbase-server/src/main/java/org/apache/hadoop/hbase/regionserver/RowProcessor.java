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
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import com.google.protobuf.Message;

/**
 * Defines the procedure to atomically perform multiple scans and mutations
 * on a HRegion.
 *
 * This is invoked by HRegion#processRowsWithLocks().
 * This class performs scans and generates mutations and WAL edits.
 * The locks and MVCC will be handled by HRegion.
 *
 * The RowProcessor user code could have data that needs to be 
 * sent across for proper initialization at the server side. The generic type 
 * parameter S is the type of the request data sent to the server.
 * The generic type parameter T is the return type of RowProcessor.getResult().
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RowProcessor<S extends Message, T extends Message> {

  /**
   * Rows to lock while operation.
   * They have to be sorted with <code>RowProcessor</code>
   * to avoid deadlock.
   */
  Collection<byte[]> getRowsToLock();

  /**
   * Obtain the processing result. All row processor implementations must
   * implement this, even if the method is simply returning an empty
   * Message.
   */
  T getResult();

  /**
   * Is this operation read only? If this is true, process() should not add
   * any mutations or it throws IOException.
   * @return ture if read only operation
   */
  boolean readOnly();

  /**
   * HRegion handles the locks and MVCC and invokes this method properly.
   *
   * You should override this to create your own RowProcessor.
   *
   * If you are doing read-modify-write here, you should consider using
   * <code>IsolationLevel.READ_UNCOMMITTED</code> for scan because
   * we advance MVCC after releasing the locks for optimization purpose.
   *
   * @param now the current system millisecond
   * @param region the HRegion
   * @param mutations the output mutations to apply to memstore
   * @param walEdit the output WAL edits to apply to write ahead log
   */
  void process(long now,
               HRegion region,
               List<Mutation> mutations,
               WALEdit walEdit) throws IOException;

  /**
   * The hook to be executed before process().
   *
   * @param region the HRegion
   * @param walEdit the output WAL edits to apply to write ahead log
   */
  void preProcess(HRegion region, WALEdit walEdit) throws IOException;

  /**
   * The hook to be executed after the process() but before applying the Mutations to region. Also
   * by the time this hook is been called, mvcc transaction is started.
   * @param region
   * @param walEdit the output WAL edits to apply to write ahead log
   * @throws IOException
   */
  void preBatchMutate(HRegion region, WALEdit walEdit) throws IOException;

  /**
   * The hook to be executed after the process() and applying the Mutations to region. The
   * difference of this one with {@link #postProcess(HRegion, WALEdit, boolean)} is this hook will
   * be executed before the mvcc transaction completion.
   * @param region
   * @throws IOException
   */
  void postBatchMutate(HRegion region) throws IOException;

  /**
   * The hook to be executed after process() and applying the Mutations to region.
   *
   * @param region the HRegion
   * @param walEdit the output WAL edits to apply to write ahead log
   * @param success true if batch operation is successful otherwise false.
   */
  void postProcess(HRegion region, WALEdit walEdit, boolean success) throws IOException;

  /**
   * @return The cluster ids that have the change.
   */
  List<UUID> getClusterIds();

  /**
   * Human readable name of the processor
   * @return The name of the processor
   */
  String getName();

  /**
   * This method should return any additional data that is needed on the
   * server side to construct the RowProcessor. The server will pass this to
   * the {@link #initialize(Message msg)} method. If there is no RowProcessor
   * specific data then null should be returned.
   * @return the PB message
   * @throws IOException
   */
  S getRequestData() throws IOException;

  /**
   * This method should initialize any field(s) of the RowProcessor with
   * a parsing of the passed message bytes (used on the server side).
   * @param msg
   * @throws IOException
   */
  void initialize(S msg) throws IOException;

  /**
   * @return The {@link Durability} to use
   */
  Durability useDurability();
}

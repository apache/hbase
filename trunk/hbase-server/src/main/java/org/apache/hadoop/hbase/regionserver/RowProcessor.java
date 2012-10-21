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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

@InterfaceAudience.Public
@InterfaceStability.Evolving

/**
 * Defines the procedure to atomically perform multiple scans and mutations
 * on a HRegion.
 *
 * This is invoked by {@link HRegion#processRowsWithLocks()}.
 * This class performs scans and generates mutations and WAL edits.
 * The locks and MVCC will be handled by HRegion.
 *
 * The generic type parameter T is the return type of
 * RowProcessor.getResult().
 */
public interface RowProcessor<T> {

  /**
   * Rows to lock while operation.
   * They have to be sorted with <code>RowProcessor</code>
   * to avoid deadlock.
   */
  Collection<byte[]> getRowsToLock();

  /**
   * Obtain the processing result
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
               List<KeyValue> mutations,
               WALEdit walEdit) throws IOException;

  /**
   * The hook to be executed before process().
   *
   * @param region the HRegion
   * @param walEdit the output WAL edits to apply to write ahead log
   */
  void preProcess(HRegion region, WALEdit walEdit) throws IOException;

  /**
   * The hook to be executed after process().
   *
   * @param region the HRegion
   * @param walEdit the output WAL edits to apply to write ahead log
   */
  void postProcess(HRegion region, WALEdit walEdit) throws IOException;


  /**
   * @return The replication cluster id.
   */
  UUID getClusterId();

  /**
   * Human readable name of the processor
   * @return The name of the processor
   */
  String getName();
}

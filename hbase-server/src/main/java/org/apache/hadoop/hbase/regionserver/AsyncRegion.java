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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.wal.WALSplitter;

/**
 * Async version of Region. Support non-blocking operations and can pass more information into
 * the operations.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface AsyncRegion extends Region {

  void getRowLock(RegionOperationContext<RowLock> context, byte[] row, boolean readLock);

  void append(RegionOperationContext<Result> context, Append append, long nonceGroup, long nonce);

  void batchMutate(RegionOperationContext<OperationStatus[]> context, Mutation[] mutations,
      long nonceGroup, long nonce);

  void batchReplay(RegionOperationContext<OperationStatus[]> context, WALSplitter.MutationReplay[] mutations,
      long replaySeqId);

  void checkAndMutate(RegionOperationContext<Boolean> context, byte [] row, byte [] family,
      byte [] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Mutation mutation,
      boolean writeToWAL);

  void checkAndRowMutate(RegionOperationContext<Boolean> context, byte [] row, byte [] family,
      byte [] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator,
      RowMutations mutations, boolean writeToWAL);

  void delete(RegionOperationContext<Void> context, Delete delete);

  void get(RegionOperationContext<Result> context, Get get);

  void get(RegionOperationContext<List<Cell>> context, Get get, boolean withCoprocessor);

  void get(RegionOperationContext<List<Cell>> context, Get get, boolean withCoprocessor,
      long nonceGroup, long nonce);

  void getScanner(RegionOperationContext<RegionScanner> context, Scan scan);

  void getScanner(RegionOperationContext<RegionScanner> context, Scan scan,
      List<KeyValueScanner> additionalScanners);

  void increment(RegionOperationContext<Result> context, Increment increment, long nonceGroup,
      long nonce);

  void mutateRow(RegionOperationContext<Void> context, RowMutations mutations);

  void mutateRowsWithLocks(RegionOperationContext<Void> context, Collection<Mutation> mutations,
      Collection<byte[]> rowsToLock, long nonceGroup, long nonce);

  void processRowsWithLocks(RegionOperationContext<Void> context, RowProcessor<?,?> processor);

  void processRowsWithLocks(RegionOperationContext<Void> context, RowProcessor<?,?> processor,
      long nonceGroup, long nonce);

  void processRowsWithLocks(RegionOperationContext<Void> context, RowProcessor<?,?> processor,
      long timeout, long nonceGroup, long nonce);

  void put(RegionOperationContext<Void> context, Put put);

}

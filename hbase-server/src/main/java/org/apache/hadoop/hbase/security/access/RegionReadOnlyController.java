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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class RegionReadOnlyController
  implements RegionCoprocessor, RegionObserver, ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(RegionReadOnlyController.class);
  private volatile boolean globalReadOnlyEnabled;

  private void internalReadOnlyGuard() throws DoNotRetryIOException {
    if (this.globalReadOnlyEnabled) {
      throw new DoNotRetryIOException("Operation not allowed in Read-Only Mode");
    }
  }

  private boolean isOnMeta(final ObserverContext<? extends RegionCoprocessorEnvironment> c) {
    return TableName.isMetaTableName(c.getEnvironment().getRegionInfo().getTable());
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.globalReadOnlyEnabled =
      env.getConfiguration().getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    boolean maybeUpdatedConfValue = conf.getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
    if (this.globalReadOnlyEnabled != maybeUpdatedConfValue) {
      this.globalReadOnlyEnabled = maybeUpdatedConfValue;
      LOG.info("Config {} has been dynamically changed to {}.",
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, this.globalReadOnlyEnabled);
    }
  }

  @Override
  public void preFlushScannerOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preFlushScannerOpen(c, store, options, tracker);
  }

  @Override
  public void preFlush(final ObserverContext<? extends RegionCoprocessorEnvironment> c,
    FlushLifeCycleTracker tracker) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preFlush(c, tracker);
  }

  @Override
  public InternalScanner preFlush(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preFlush(c, store, scanner, tracker);
  }

  @Override
  public void preMemStoreCompaction(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store) throws IOException {
    internalReadOnlyGuard();
    RegionObserver.super.preMemStoreCompaction(c, store);
  }

  @Override
  public void preMemStoreCompactionCompactScannerOpen(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, Store store, ScanOptions options)
    throws IOException {
    internalReadOnlyGuard();
    RegionObserver.super.preMemStoreCompactionCompactScannerOpen(c, store, options);
  }

  @Override
  public InternalScanner preMemStoreCompactionCompact(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner)
    throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preMemStoreCompactionCompact(c, store, scanner);
  }

  @Override
  public void preCompactSelection(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, List<? extends StoreFile> candidates, CompactionLifeCycleTracker tracker)
    throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preCompactSelection(c, store, candidates, tracker);
  }

  @Override
  public void preCompactScannerOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
    CompactionRequest request) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preCompactScannerOpen(c, store, scanType, options, tracker, request);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
    CompactionRequest request) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCompact(c, store, scanner, scanType, tracker, request);
  }

  @Override
  public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
    WALEdit edit, Durability durability) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.prePut(c, put, edit, durability);
  }

  @Override
  public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
    WALEdit edit) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.prePut(c, put, edit);
  }

  @Override
  public void preDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c, Delete delete,
    WALEdit edit, Durability durability) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preDelete(c, delete, edit, durability);
  }

  @Override
  public void preDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c, Delete delete,
    WALEdit edit) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preDelete(c, delete, edit);
  }

  @Override
  public void preBatchMutate(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preBatchMutate(c, miniBatchOp);
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
    Put put, boolean result) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPut(c, row, family, qualifier, op, comparator, put,
      result);
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Put put, boolean result) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPut(c, row, filter, put, result);
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
    byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put, boolean result)
    throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPutAfterRowLock(c, row, family, qualifier, op,
      comparator, put, result);
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, Filter filter, Put put,
    boolean result) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
    Delete delete, boolean result) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Delete delete, boolean result) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
    byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Delete delete,
    boolean result) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, Filter filter,
    Delete delete, boolean result) throws IOException {
    if (!isOnMeta(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
  }

  @Override
  public CheckAndMutateResult preCheckAndMutate(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, CheckAndMutate checkAndMutate,
    CheckAndMutateResult result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndMutate(c, checkAndMutate, result);
  }

  @Override
  public CheckAndMutateResult preCheckAndMutateAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, CheckAndMutate checkAndMutate,
    CheckAndMutateResult result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndMutateAfterRowLock(c, checkAndMutate, result);
  }

  @Override
  public Result preAppend(ObserverContext<? extends RegionCoprocessorEnvironment> c, Append append)
    throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preAppend(c, append);
  }

  @Override
  public Result preAppend(ObserverContext<? extends RegionCoprocessorEnvironment> c, Append append,
    WALEdit edit) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preAppend(c, append, edit);
  }

  @Override
  public Result preAppendAfterRowLock(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Append append) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preAppendAfterRowLock(c, append);
  }

  @Override
  public Result preIncrement(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Increment increment) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preIncrement(c, increment);
  }

  @Override
  public Result preIncrement(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Increment increment, WALEdit edit) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preIncrement(c, increment, edit);
  }

  @Override
  public Result preIncrementAfterRowLock(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Increment increment) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preIncrementAfterRowLock(c, increment);
  }

  @Override
  public void preReplayWALs(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    RegionInfo info, Path edits) throws IOException {
    internalReadOnlyGuard();
    RegionObserver.super.preReplayWALs(ctx, info, edits);
  }

  @Override
  public void preBulkLoadHFile(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    List<Pair<byte[], String>> familyPaths) throws IOException {
    internalReadOnlyGuard();
    RegionObserver.super.preBulkLoadHFile(ctx, familyPaths);
  }

  @Override
  public void preCommitStoreFile(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    byte[] family, List<Pair<Path, Path>> pairs) throws IOException {
    internalReadOnlyGuard();
    RegionObserver.super.preCommitStoreFile(ctx, family, pairs);
  }

  @Override
  public void preWALAppend(ObserverContext<? extends RegionCoprocessorEnvironment> ctx, WALKey key,
    WALEdit edit) throws IOException {
    // Only allow this operation for meta table
    if (!TableName.isMetaTableName(key.getTableName())) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preWALAppend(ctx, key, edit);
  }
}

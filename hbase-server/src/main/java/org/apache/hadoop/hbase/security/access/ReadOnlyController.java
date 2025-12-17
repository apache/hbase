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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReadOnlyController implements MasterCoprocessor, RegionCoprocessor, MasterObserver,
  RegionObserver, RegionServerCoprocessor, RegionServerObserver, EndpointObserver, BulkLoadObserver,
  ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyController.class);
  private MasterServices masterServices;

  private volatile boolean globalReadOnlyEnabled;

  private void internalReadOnlyGuard() throws DoNotRetryIOException {
    if (this.globalReadOnlyEnabled) {
      throw new DoNotRetryIOException("Operation not allowed in Read-Only Mode");
    }
  }

  private boolean
    isOperationOnNonMetaTable(final ObserverContext<? extends RegionCoprocessorEnvironment> c) {
    return !c.getEnvironment().getRegionInfo().getTable().equals(TableName.META_TABLE_NAME);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof MasterCoprocessorEnvironment) {
      this.masterServices = ((MasterCoprocessorEnvironment) env).getMasterServices();
      LOG.info("ReadOnlyController obtained MasterServices reference from start().");
    } else {
      LOG.debug("ReadOnlyController loaded in a non-Master environment. "
        + "File system operations for read-only state will not work.");
    }
    this.globalReadOnlyEnabled =
      env.getConfiguration().getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  /* ---- RegionObserver Overrides ---- */
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void preFlushScannerOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preFlushScannerOpen(c, store, options, tracker);
  }

  @Override
  public void preFlush(final ObserverContext<? extends RegionCoprocessorEnvironment> c,
    FlushLifeCycleTracker tracker) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preFlush(c, tracker);
  }

  @Override
  public InternalScanner preFlush(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
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
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preCompactSelection(c, store, candidates, tracker);
  }

  @Override
  public void preCompactScannerOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
    CompactionRequest request) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preCompactScannerOpen(c, store, scanType, options, tracker, request);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
    CompactionRequest request) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCompact(c, store, scanner, scanType, tracker, request);
  }

  @Override
  public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
    WALEdit edit, Durability durability) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.prePut(c, put, edit, durability);
  }

  @Override
  public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
    WALEdit edit) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.prePut(c, put, edit);
  }

  @Override
  public void preDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c, Delete delete,
    WALEdit edit, Durability durability) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preDelete(c, delete, edit, durability);
  }

  @Override
  public void preDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c, Delete delete,
    WALEdit edit) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preDelete(c, delete, edit);
  }

  @Override
  public void preBatchMutate(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preBatchMutate(c, miniBatchOp);
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
    Put put, boolean result) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPut(c, row, family, qualifier, op, comparator, put,
      result);
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Put put, boolean result) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPut(c, row, filter, put, result);
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
    byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put, boolean result)
    throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPutAfterRowLock(c, row, family, qualifier, op,
      comparator, put, result);
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, Filter filter, Put put,
    boolean result) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
    Delete delete, boolean result) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Delete delete, boolean result) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
    byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Delete delete,
    boolean result) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
      internalReadOnlyGuard();
    }
    return RegionObserver.super.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, Filter filter,
    Delete delete, boolean result) throws IOException {
    if (isOperationOnNonMetaTable(c)) {
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
    if (!key.getTableName().equals(TableName.META_TABLE_NAME)) {
      internalReadOnlyGuard();
    }
    RegionObserver.super.preWALAppend(ctx, key, edit);
  }

  /* ---- MasterObserver Overrides ---- */
  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public TableDescriptor preCreateTableRegionsInfos(
    ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc) throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preCreateTableRegionsInfos(ctx, desc);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCreateTable(ctx, desc, regions);
  }

  @Override
  public void preCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCreateTableAction(ctx, desc, regions);
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteTable(ctx, tableName);
  }

  @Override
  public void preDeleteTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteTableAction(ctx, tableName);
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preTruncateTable(ctx, tableName);
  }

  @Override
  public void preTruncateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preTruncateTableAction(ctx, tableName);
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Override
  public String preModifyTableStoreFileTracker(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, String dstSFT) throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preModifyTableStoreFileTracker(ctx, tableName, dstSFT);
  }

  @Override
  public String preModifyColumnFamilyStoreFileTracker(
    ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] family,
    String dstSFT) throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preModifyColumnFamilyStoreFileTracker(ctx, tableName, family,
      dstSFT);
  }

  @Override
  public void preModifyTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preModifyTableAction(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Override
  public void preSplitRegion(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName,
    byte[] splitRow) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegion(c, tableName, splitRow);
  }

  @Override
  public void preSplitRegionAction(ObserverContext<MasterCoprocessorEnvironment> c,
    TableName tableName, byte[] splitRow) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegionAction(c, tableName, splitRow);
  }

  @Override
  public void preSplitRegionBeforeMETAAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    byte[] splitKey, List<Mutation> metaEntries) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegionBeforeMETAAction(ctx, splitKey, metaEntries);
  }

  @Override
  public void preSplitRegionAfterMETAAction(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegionAfterMETAAction(ctx);
  }

  @Override
  public void preTruncateRegion(ObserverContext<MasterCoprocessorEnvironment> c,
    RegionInfo regionInfo) {
    try {
      internalReadOnlyGuard();
    } catch (IOException e) {
      LOG.info("Region truncation of region {} not allowed in read-only mode",
        regionInfo.getRegionNameAsString());
    }
    MasterObserver.super.preTruncateRegion(c, regionInfo);
  }

  @Override
  public void preTruncateRegionAction(ObserverContext<MasterCoprocessorEnvironment> c,
    RegionInfo regionInfo) {
    try {
      internalReadOnlyGuard();
    } catch (IOException e) {
      LOG.info("Region truncation of region {} not allowed in read-only mode",
        regionInfo.getRegionNameAsString());
    }
    MasterObserver.super.preTruncateRegionAction(c, regionInfo);
  }

  @Override
  public void preMergeRegionsAction(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final RegionInfo[] regionsToMerge) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMergeRegionsAction(ctx, regionsToMerge);
  }

  @Override
  public void preMergeRegionsCommitAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    RegionInfo[] regionsToMerge, List<Mutation> metaEntries) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMergeRegionsCommitAction(ctx, regionsToMerge, metaEntries);
  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCloneSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Override
  public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRestoreSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Override
  public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteSnapshot(ctx, snapshot);
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor ns) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCreateNamespace(ctx, ns);
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor currentNsDescriptor, NamespaceDescriptor newNsDescriptor)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preModifyNamespace(ctx, currentNsDescriptor, newNsDescriptor);
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String namespace) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteNamespace(ctx, namespace);
  }

  @Override
  public void preMasterStoreFlush(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMasterStoreFlush(ctx);
  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
    GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetUserQuota(ctx, userName, quotas);
  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
    TableName tableName, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetUserQuota(ctx, userName, tableName, quotas);
  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
    String namespace, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetUserQuota(ctx, userName, namespace, quotas);
  }

  @Override
  public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetTableQuota(ctx, tableName, quotas);
  }

  @Override
  public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String namespace, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetNamespaceQuota(ctx, namespace, quotas);
  }

  @Override
  public void preSetRegionServerQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String regionServer, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetRegionServerQuota(ctx, regionServer, quotas);
  }

  @Override
  public void preMergeRegions(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final RegionInfo[] regionsToMerge) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMergeRegions(ctx, regionsToMerge);
  }

  @Override
  public void preMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMoveServersAndTables(ctx, servers, tables, targetGroup);
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<Address> servers, String targetGroup) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMoveServers(ctx, servers, targetGroup);
  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<TableName> tables, String targetGroup) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMoveTables(ctx, tables, targetGroup);
  }

  @Override
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preAddRSGroup(ctx, name);
  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRemoveRSGroup(ctx, name);
  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName,
    BalanceRequest request) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preBalanceRSGroup(ctx, groupName, request);
  }

  @Override
  public void preRemoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<Address> servers) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRemoveServers(ctx, servers);
  }

  @Override
  public void preRenameRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String oldName,
    String newName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRenameRSGroup(ctx, oldName, newName);
  }

  @Override
  public void preUpdateRSGroupConfig(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String groupName, Map<String, String> configuration) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preUpdateRSGroupConfig(ctx, groupName, configuration);
  }

  @Override
  public void preAddReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId, ReplicationPeerConfig peerConfig) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preAddReplicationPeer(ctx, peerId, peerConfig);
  }

  @Override
  public void preRemoveReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRemoveReplicationPeer(ctx, peerId);
  }

  @Override
  public void preEnableReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preEnableReplicationPeer(ctx, peerId);
  }

  @Override
  public void preDisableReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDisableReplicationPeer(ctx, peerId);
  }

  @Override
  public void preUpdateReplicationPeerConfig(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId, ReplicationPeerConfig peerConfig) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preUpdateReplicationPeerConfig(ctx, peerId, peerConfig);
  }

  @Override
  public void preTransitReplicationPeerSyncReplicationState(
    ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId, SyncReplicationState state)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preTransitReplicationPeerSyncReplicationState(ctx, peerId, state);
  }

  @Override
  public void preGrant(ObserverContext<MasterCoprocessorEnvironment> ctx,
    UserPermission userPermission, boolean mergeExistingPermissions) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preGrant(ctx, userPermission, mergeExistingPermissions);
  }

  @Override
  public void preRevoke(ObserverContext<MasterCoprocessorEnvironment> ctx,
    UserPermission userPermission) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRevoke(ctx, userPermission);
  }

  /* ---- RegionServerObserver Overrides ---- */
  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preRollWALWriterRequest(ctx);
  }

  @Override
  public void preExecuteProcedures(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preExecuteProcedures(ctx);
  }

  @Override
  public void preReplicationSinkBatchMutate(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
    AdminProtos.WALEntry walEntry, Mutation mutation) throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preReplicationSinkBatchMutate(ctx, walEntry, mutation);
  }

  @Override
  public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preReplicateLogEntries(ctx);
  }

  /* ---- EndpointObserver Overrides ---- */
  @Override
  public Message preEndpointInvocation(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    Service service, String methodName, Message request) throws IOException {
    internalReadOnlyGuard();
    return EndpointObserver.super.preEndpointInvocation(ctx, service, methodName, request);
  }

  /* ---- BulkLoadObserver Overrides ---- */
  @Override
  public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    BulkLoadObserver.super.prePrepareBulkLoad(ctx);
  }

  @Override
  public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    BulkLoadObserver.super.preCleanupBulkLoad(ctx);
  }

  private void manageActiveClusterIdFile(boolean newValue) {
    MasterFileSystem mfs = this.masterServices.getMasterFileSystem();
    FileSystem fs = mfs.getFileSystem();
    Path rootDir = mfs.getRootDir();
    Path activeClusterFile = new Path(rootDir, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME);

    try {
      if (newValue) {
        // ENABLING READ-ONLY (false -> true), delete the active cluster file.
        LOG.debug("Global read-only mode is being ENABLED. Deleting active cluster file: {}",
          activeClusterFile);
        try {
          fs.delete(activeClusterFile, false);
          LOG.info("Successfully deleted active cluster file: {}", activeClusterFile);
        } catch (IOException e) {
          LOG.error(
            "Failed to delete active cluster file: {}. "
              + "Read-only flag will be updated, but file system state is inconsistent.",
            activeClusterFile);
        }
      } else {
        // DISABLING READ-ONLY (true -> false), create the active cluster file id file
        int wait = mfs.getConfiguration().getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
        FSUtils.setActiveClusterSuffix(fs, rootDir, mfs.getSuffixFileDataToWrite(), wait);
      }
    } catch (IOException e) {
      // We still update the flag, but log that the operation failed.
      LOG.error("Failed to perform file operation for read-only switch. "
        + "Flag will be updated, but file system state may be inconsistent.", e);
    }
  }

  /* ---- ConfigurationObserver Overrides ---- */
  public void onConfigurationChange(Configuration conf) {
    boolean maybeUpdatedConfValue = conf.getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
    if (this.globalReadOnlyEnabled != maybeUpdatedConfValue) {
      if (this.masterServices != null) {
        manageActiveClusterIdFile(maybeUpdatedConfValue);
      } else {
        LOG.debug("Global R/O flag changed, but not running on master");
      }
      this.globalReadOnlyEnabled = maybeUpdatedConfValue;
      LOG.info("Config {} has been dynamically changed to {}.",
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, this.globalReadOnlyEnabled);
    }
  }
}

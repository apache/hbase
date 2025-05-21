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
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
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
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
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
  private volatile boolean globalReadOnlyEnabled;

  private void internalReadOnlyGuard() throws IOException {
    if (this.globalReadOnlyEnabled) {
      throw new IOException("Operation not allowed in Read-Only Mode");
    }
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

  /* ---- RegionObserver Overrides ---- */
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
    WALEdit edit) throws IOException {
    TableName tableName = c.getEnvironment().getRegionInfo().getTable();
    if (tableName.isSystemTable()) {
      return;
    }
    internalReadOnlyGuard();
  }

  @Override
  public void preDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c, Delete delete,
    WALEdit edit) throws IOException {
    internalReadOnlyGuard();
  }

  @Override
  public void preBatchMutate(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    TableName tableName = c.getEnvironment().getRegionInfo().getTable();
    if (tableName.isSystemTable()) {
      return;
    }
    internalReadOnlyGuard();
  }

  @Override
  public void preFlush(final ObserverContext<? extends RegionCoprocessorEnvironment> c,
    FlushLifeCycleTracker tracker) throws IOException {
    internalReadOnlyGuard();
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
    Put put, boolean result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndPut(c, row, family, qualifier, op, comparator, put,
      result);
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Put put, boolean result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndPut(c, row, filter, put, result);
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
    byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put, boolean result)
    throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndPutAfterRowLock(c, row, family, qualifier, op,
      comparator, put, result);
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, Filter filter, Put put,
    boolean result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndPutAfterRowLock(c, row, filter, put, result);
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
    Delete delete, boolean result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete,
      result);
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    byte[] row, Filter filter, Delete delete, boolean result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndDelete(c, row, filter, delete, result);
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
    byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Delete delete,
    boolean result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, op,
      comparator, delete, result);
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
    ObserverContext<? extends RegionCoprocessorEnvironment> c, byte[] row, Filter filter,
    Delete delete, boolean result) throws IOException {
    internalReadOnlyGuard();
    return RegionObserver.super.preCheckAndDeleteAfterRowLock(c, row, filter, delete, result);
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
  public void preBulkLoadHFile(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    List<Pair<byte[], String>> familyPaths) throws IOException {
    internalReadOnlyGuard();
    RegionObserver.super.preBulkLoadHFile(ctx, familyPaths);
  }

  /* ---- MasterObserver Overrides ---- */
  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCreateTable(ctx, desc, regions);
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
  public void preMergeRegionsAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    RegionInfo[] regionsToMerge) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMergeRegionsAction(ctx, regionsToMerge);
  }

  /* ---- RegionServerObserver Overrides ---- */
  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preRollWALWriterRequest(ctx);
  }

  @Override
  public void preClearCompactionQueues(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preClearCompactionQueues(ctx);
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
  public void preClearRegionBlockCache(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    RegionServerObserver.super.preClearRegionBlockCache(ctx);
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

  /* ---- ConfigurationObserver Overrides ---- */
  @Override
  public void onConfigurationChange(Configuration conf) {
    boolean maybeUpdatedConfValue = conf.getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
    if (this.globalReadOnlyEnabled != maybeUpdatedConfValue) {
      this.globalReadOnlyEnabled = maybeUpdatedConfValue;
      LOG.info("Config {} has been dynamically changed to {}",
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, this.globalReadOnlyEnabled);
    }
  }
}

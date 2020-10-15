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

package org.apache.hadoop.hbase.tool.coprocessor;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class Branch1CoprocessorMethods extends CoprocessorMethods {
  public Branch1CoprocessorMethods() {
    addMethods();
  }

  /*
   * This list of methods was generated from HBase 1.4.4.
   */
  private void addMethods() {
    /* BulkLoadObserver */

    addMethod("prePrepareBulkLoad",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadRequest");

    addMethod("preCleanupBulkLoad",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadRequest");

    /* EndpointObserver */

    addMethod("postEndpointInvocation",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "com.google.protobuf.Service",
        "java.lang.String",
        "com.google.protobuf.Message",
        "com.google.protobuf.Message.Builder");

    addMethod("preEndpointInvocation",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "com.google.protobuf.Service",
        "java.lang.String",
        "com.google.protobuf.Message");

    /* MasterObserver */

    addMethod("preCreateTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HTableDescriptor",
        "org.apache.hadoop.hbase.HRegionInfo[]");

    addMethod("postCreateTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HTableDescriptor",
        "org.apache.hadoop.hbase.HRegionInfo[]");

    addMethod("preDeleteTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postDeleteTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preDeleteTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preMove",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.ServerName",
        "org.apache.hadoop.hbase.ServerName");

    addMethod("preCreateTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HTableDescriptor",
        "org.apache.hadoop.hbase.HRegionInfo[]");

    addMethod("postCreateTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HTableDescriptor",
        "org.apache.hadoop.hbase.HRegionInfo[]");

    addMethod("postMove",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.ServerName",
        "org.apache.hadoop.hbase.ServerName");

    addMethod("postDeleteTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preTruncateTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postTruncateTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preTruncateTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postTruncateTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preModifyTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("postModifyTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("preModifyTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("postModifyTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("preAddColumn",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("postAddColumn",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("preAddColumnHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("postAddColumnHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("preModifyColumn",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("postModifyColumn",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("preModifyColumnHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("postModifyColumnHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.HColumnDescriptor");

    addMethod("preDeleteColumn",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "byte[]");

    addMethod("postDeleteColumn",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "byte[]");

    addMethod("preDeleteColumnHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "byte[]");

    addMethod("postDeleteColumnHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "byte[]");

    addMethod("preEnableTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postEnableTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preEnableTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postEnableTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preDisableTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postDisableTable",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preDisableTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postDisableTableHandler",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preAbortProcedure",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.procedure2.ProcedureExecutor",
        "long");

    addMethod("postAbortProcedure",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("preListProcedures",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postListProcedures",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List");

    addMethod("preAssign",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo");

    addMethod("postAssign",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo");

    addMethod("preUnassign",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "boolean");

    addMethod("postUnassign",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "boolean");

    addMethod("preRegionOffline",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo");

    addMethod("postRegionOffline",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo");

    addMethod("preBalance",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postBalance",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List");

    addMethod("preSetSplitOrMergeEnabled",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "boolean",
        "org.apache.hadoop.hbase.client.Admin.MasterSwitchType");

    addMethod("postSetSplitOrMergeEnabled",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "boolean",
        "org.apache.hadoop.hbase.client.Admin.MasterSwitchType");

    addMethod("preBalanceSwitch",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "boolean");

    addMethod("postBalanceSwitch",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "boolean",
        "boolean");

    addMethod("preShutdown",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("preStopMaster",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postStartMaster",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("preMasterInitialization",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("preSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("postSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("preListSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription");

    addMethod("postListSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription");

    addMethod("preCloneSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("postCloneSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("preRestoreSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("postRestoreSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription",
        "org.apache.hadoop.hbase.HTableDescriptor");

    addMethod("preDeleteSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription");

    addMethod("postDeleteSnapshot",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription");

    addMethod("preGetTableDescriptors",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "java.util.List");

    addMethod("preGetTableDescriptors",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "java.util.List",
        "java.lang.String");

    addMethod("postGetTableDescriptors",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "java.util.List",
        "java.lang.String");

    addMethod("postGetTableDescriptors",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List");

    addMethod("preGetTableNames",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "java.lang.String");

    addMethod("postGetTableNames",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "java.lang.String");

    addMethod("preCreateNamespace",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.NamespaceDescriptor");

    addMethod("postCreateNamespace",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.NamespaceDescriptor");

    addMethod("preDeleteNamespace",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("postDeleteNamespace",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("preModifyNamespace",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.NamespaceDescriptor");

    addMethod("postModifyNamespace",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.NamespaceDescriptor");

    addMethod("preGetNamespaceDescriptor",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("postGetNamespaceDescriptor",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.NamespaceDescriptor");

    addMethod("preListNamespaceDescriptors",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List");

    addMethod("postListNamespaceDescriptors",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List");

    addMethod("preTableFlush",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("postTableFlush",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName");

    addMethod("preSetUserQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "java.lang.String",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("preSetUserQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("preSetUserQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("postSetUserQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "java.lang.String",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("postSetUserQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("postSetUserQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("preSetTableQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("postSetTableQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.TableName",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("preSetNamespaceQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("postSetNamespaceQuota",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas");

    addMethod("preDispatchMerge",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.HRegionInfo");

    addMethod("postDispatchMerge",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.HRegionInfo");

    addMethod("preGetClusterStatus",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postGetClusterStatus",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.ClusterStatus");

    addMethod("preClearDeadServers",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postClearDeadServers",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "java.util.List");

    addMethod("preMoveServers",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set",
        "java.lang.String");

    addMethod("postMoveServers",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set",
        "java.lang.String");

    addMethod("preMoveTables",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set",
        "java.lang.String");

    addMethod("postMoveTables",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set",
        "java.lang.String");

    addMethod("preMoveServersAndTables",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set",
        "java.util.Set",
        "java.lang.String");

    addMethod("postMoveServersAndTables",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set",
        "java.util.Set",
        "java.lang.String");

    addMethod("preAddRSGroup",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("postAddRSGroup",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("preRemoveRSGroup",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("postRemoveRSGroup",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("preRemoveServers",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set");

    addMethod("postRemoveServers",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.Set");

    addMethod("preBalanceRSGroup",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String");

    addMethod("postBalanceRSGroup",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.lang.String",
        "boolean");

    /* RegionObserver */

    addMethod("preOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postLogReplay",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("preFlushScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.KeyValueScanner",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "long");

    addMethod("preFlushScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.KeyValueScanner",
        "org.apache.hadoop.hbase.regionserver.InternalScanner");

    addMethod("preFlush",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.InternalScanner");

    addMethod("preFlush",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postFlush",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.StoreFile");

    addMethod("postFlush",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("preCompactSelection",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "java.util.List");

    addMethod("preCompactSelection",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "java.util.List",
        "org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest");

    addMethod("postCompactSelection",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "com.google.common.collect.ImmutableList");

    addMethod("postCompactSelection",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "com.google.common.collect.ImmutableList",
        "org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest");

    addMethod("preCompact",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "org.apache.hadoop.hbase.regionserver.ScanType");

    addMethod("preCompact",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "org.apache.hadoop.hbase.regionserver.ScanType",
        "org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest");

    addMethod("preClose",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "boolean");

    addMethod("preCompactScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "java.util.List",
        "org.apache.hadoop.hbase.regionserver.ScanType",
        "long",
        "org.apache.hadoop.hbase.regionserver.InternalScanner");

    addMethod("preCompactScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "java.util.List",
        "org.apache.hadoop.hbase.regionserver.ScanType",
        "long",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest",
        "long");

    addMethod("preCompactScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "java.util.List",
        "org.apache.hadoop.hbase.regionserver.ScanType",
        "long",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest");

    addMethod("postCompact",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.StoreFile");

    addMethod("postCompact",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.regionserver.StoreFile",
        "org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest");

    addMethod("preSplit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]");

    addMethod("preSplit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postSplit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region");

    addMethod("preSplitBeforePONR",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "java.util.List");

    addMethod("preSplitAfterPONR",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("preRollBackSplit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postRollBackSplit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postCompleteSplit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postClose",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "boolean");

    addMethod("preGetClosestRowBefore",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.client.Result");

    addMethod("postGetClosestRowBefore",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.client.Result");

    addMethod("preGetOp",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Get",
        "java.util.List");

    addMethod("postGetOp",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Get",
        "java.util.List");

    addMethod("preExists",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Get",
        "boolean");

    addMethod("postExists",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Get",
        "boolean");

    addMethod("prePut",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Put",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit",
        "org.apache.hadoop.hbase.client.Durability");

    addMethod("postPut",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Put",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit",
        "org.apache.hadoop.hbase.client.Durability");

    addMethod("preDelete",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Delete",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit",
        "org.apache.hadoop.hbase.client.Durability");

    addMethod("prePrepareTimeStampForDeleteVersion",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Mutation",
        "org.apache.hadoop.hbase.Cell",
        "byte[]",
        "org.apache.hadoop.hbase.client.Get");

    addMethod("postDelete",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Delete",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit",
        "org.apache.hadoop.hbase.client.Durability");

    addMethod("preBatchMutate",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress");

    addMethod("postBatchMutate",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress");

    addMethod("postStartRegionOperation",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region.Operation");

    addMethod("postCloseRegionOperation",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region.Operation");

    addMethod("postBatchMutateIndispensably",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress",
        "boolean");

    addMethod("preCheckAndPut",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.filter.CompareFilter.CompareOp",
        "org.apache.hadoop.hbase.filter.ByteArrayComparable",
        "org.apache.hadoop.hbase.client.Put",
        "boolean");

    addMethod("preCheckAndPutAfterRowLock",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.filter.CompareFilter.CompareOp",
        "org.apache.hadoop.hbase.filter.ByteArrayComparable",
        "org.apache.hadoop.hbase.client.Put",
        "boolean");

    addMethod("postCheckAndPut",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.filter.CompareFilter.CompareOp",
        "org.apache.hadoop.hbase.filter.ByteArrayComparable",
        "org.apache.hadoop.hbase.client.Put",
        "boolean");

    addMethod("preCheckAndDelete",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.filter.CompareFilter.CompareOp",
        "org.apache.hadoop.hbase.filter.ByteArrayComparable",
        "org.apache.hadoop.hbase.client.Delete",
        "boolean");

    addMethod("preCheckAndDeleteAfterRowLock",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.filter.CompareFilter.CompareOp",
        "org.apache.hadoop.hbase.filter.ByteArrayComparable",
        "org.apache.hadoop.hbase.client.Delete",
        "boolean");

    addMethod("postCheckAndDelete",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "org.apache.hadoop.hbase.filter.CompareFilter.CompareOp",
        "org.apache.hadoop.hbase.filter.ByteArrayComparable",
        "org.apache.hadoop.hbase.client.Delete",
        "boolean");

    addMethod("preIncrementColumnValue",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "long",
        "boolean");

    addMethod("postIncrementColumnValue",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "byte[]",
        "byte[]",
        "long",
        "boolean",
        "long");

    addMethod("preAppend",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Append");

    addMethod("preAppendAfterRowLock",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Append");

    addMethod("postAppend",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Append",
        "org.apache.hadoop.hbase.client.Result");

    addMethod("preIncrement",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Increment");

    addMethod("preIncrementAfterRowLock",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Increment");

    addMethod("postIncrement",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Increment",
        "org.apache.hadoop.hbase.client.Result");

    addMethod("preScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Scan",
        "org.apache.hadoop.hbase.regionserver.RegionScanner");

    addMethod("preStoreScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Store",
        "org.apache.hadoop.hbase.client.Scan",
        "java.util.NavigableSet",
        "org.apache.hadoop.hbase.regionserver.KeyValueScanner");

    addMethod("postScannerOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.client.Scan",
        "org.apache.hadoop.hbase.regionserver.RegionScanner");

    addMethod("preScannerNext",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "java.util.List",
        "int",
        "boolean");

    addMethod("postScannerNext",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "java.util.List",
        "int",
        "boolean");

    addMethod("postScannerFilterRow",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.InternalScanner",
        "byte[]",
        "int",
        "short",
        "boolean");

    addMethod("preScannerClose",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.InternalScanner");

    addMethod("postScannerClose",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.InternalScanner");

    addMethod("preWALRestore",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.regionserver.wal.HLogKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("preWALRestore",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.wal.WALKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("postWALRestore",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.regionserver.wal.HLogKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("postWALRestore",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.wal.WALKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("preBulkLoadHFile",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List");

    addMethod("preCommitStoreFile",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "java.util.List");

    addMethod("postCommitStoreFile",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "byte[]",
        "org.apache.hadoop.fs.Path",
        "org.apache.hadoop.fs.Path");

    addMethod("postBulkLoadHFile",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "boolean");

    addMethod("preStoreFileReaderOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.fs.FileSystem",
        "org.apache.hadoop.fs.Path",
        "org.apache.hadoop.hbase.io.FSDataInputStreamWrapper",
        "long",
        "org.apache.hadoop.hbase.io.hfile.CacheConfig",
        "org.apache.hadoop.hbase.io.Reference",
        "org.apache.hadoop.hbase.regionserver.StoreFile.Reader");

    addMethod("postStoreFileReaderOpen",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.fs.FileSystem",
        "org.apache.hadoop.fs.Path",
        "org.apache.hadoop.hbase.io.FSDataInputStreamWrapper",
        "long",
        "org.apache.hadoop.hbase.io.hfile.CacheConfig",
        "org.apache.hadoop.hbase.io.Reference",
        "org.apache.hadoop.hbase.regionserver.StoreFile.Reader");

    addMethod("postMutationBeforeWAL",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.coprocessor.RegionObserver.MutationType",
        "org.apache.hadoop.hbase.client.Mutation",
        "org.apache.hadoop.hbase.Cell",
        "org.apache.hadoop.hbase.Cell");

    addMethod("postInstantiateDeleteTracker",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.DeleteTracker");

    /* RegionServerObserver */

    addMethod("preMerge",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region");

    addMethod("preStopRegionServer",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postMerge",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region");

    addMethod("preMergeCommit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region",
        "java.util.List");

    addMethod("postMergeCommit",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region");

    addMethod("preRollBackMerge",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region");

    addMethod("postRollBackMerge",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.regionserver.Region",
        "org.apache.hadoop.hbase.regionserver.Region");

    addMethod("preRollWALWriterRequest",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postRollWALWriterRequest",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext");

    addMethod("postCreateReplicationEndPoint",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.replication.ReplicationEndpoint");

    addMethod("preReplicateLogEntries",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "org.apache.hadoop.hbase.CellScanner");

    addMethod("postReplicateLogEntries",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "java.util.List",
        "org.apache.hadoop.hbase.CellScanner");

    /* WALObserver */

    addMethod("preWALWrite",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.wal.WALKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("preWALWrite",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.regionserver.wal.HLogKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("postWALWrite",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.regionserver.wal.HLogKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("postWALWrite",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.hbase.HRegionInfo",
        "org.apache.hadoop.hbase.wal.WALKey",
        "org.apache.hadoop.hbase.regionserver.wal.WALEdit");

    addMethod("preWALRoll",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.fs.Path",
        "org.apache.hadoop.fs.Path");

    addMethod("postWALRoll",
        "org.apache.hadoop.hbase.coprocessor.ObserverContext",
        "org.apache.hadoop.fs.Path",
        "org.apache.hadoop.fs.Path");
  }
}

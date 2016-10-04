/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;

@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.CONFIG})
@InterfaceStability.Evolving
public class BaseMasterObserver implements MasterObserver {
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  /**
   * Called before a new table is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.  Called as part of create
   * table handler and it is async to the create RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preCreateTableAction(ObserverContext, HTableDescriptor, HRegionInfo[])}.
   */
  @Deprecated
  @Override
  public void preCreateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HTableDescriptor desc,
      final HRegionInfo[] regions) throws IOException {
  }

  /**
   * Called after the createTable operation has been requested.  Called as part
   * of create table RPC call.  Called as part of create table handler and
   * it is async to the create RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *   (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *   Use {@link #postCompletedCreateTableAction(ObserverContext, HTableDescriptor, HRegionInfo[])}
   */
  @Deprecated
  @Override
  public void postCreateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCompletedCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HTableDescriptor desc,
      final HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preDispatchMerge(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionA, HRegionInfo regionB) throws IOException {
  }

  @Override
  public void postDispatchMerge(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionA, HRegionInfo regionB) throws IOException {
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table handler and
   * it is async to the delete RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preDeleteTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void preDeleteTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException{
  }

  @Override
  public void preDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException{
  }

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table handler and it is async to the
   * delete RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedDeleteTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void postDeleteTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postCompletedDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table handler and it is sync
   * to the truncate RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preTruncateTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void preTruncateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {
  }

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table handler and it is sync to the
   * truncate RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedTruncateTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void postTruncateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postCompletedTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
  }

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table handler and it is async to the modify table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preModifyTableAction(ObserverContext, TableName, HTableDescriptor)}.
   */
  @Deprecated
  @Override
  public void preModifyTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HTableDescriptor htd) throws IOException {
  }

  /**
   * Called after to modifying a table's properties.  Called as part of modify
   * table handler and it is async to the modify table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *     Use {@link #postCompletedModifyTableAction(ObserverContext, TableName, HTableDescriptor)}.
   */
  @Deprecated
  @Override
  public void postModifyTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postCompletedModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preCreateNamespace(
      ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
          throws IOException {
  }

  @Override
  public void postCreateNamespace(
      ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
          throws IOException {
  }

  @Override
  public void preDeleteNamespace(
      ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
  }

  @Override
  public void postDeleteNamespace(
      ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
  }

  @Override
  public void preModifyNamespace(
      ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
          throws IOException {
  }

  @Override
  public void postModifyNamespace(
      ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
          throws IOException {
  }

  @Override
  public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
  }

  @Override
  public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {
  }

  @Override
  public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {
  }

  /**
   * Called prior to adding a new column family to the table.  Called as part of
   * add column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preAddColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preAddColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called after the new column family has been created.  Called as part of
   * add column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #postAddColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postAddColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called prior to adding a new column family to the table.  Called as part of
   * add column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *          (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>). Use
   *          {@link #preAddColumnFamilyAction(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void preAddColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preAddColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called after the new column family has been created.  Called as part of
   * add column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>). Use
   *     {@link #postCompletedAddColumnFamilyAction(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void postAddColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postCompletedAddColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called prior to modifying a column family's attributes.  Called as part of
   * modify column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preModifyColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preModifyColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called after the column family has been updated.  Called as part of modify
   * column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #postModifyColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postModifyColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called prior to modifying a column family's attributes.  Called as part of
   * modify column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *     Use {@link #preModifyColumnFamilyAction(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void preModifyColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preModifyColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called after the column family has been updated.  Called as part of modify
   * column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *   (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>). Use
   *   {@link #postCompletedModifyColumnFamilyAction(ObserverContext,TableName,HColumnDescriptor)}.
   */
  @Deprecated
  @Override
  public void postModifyColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postCompletedModifyColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
  }

  /**
   * Called prior to deleting the entire column family.  Called as part of
   * delete column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preDeleteColumnFamily(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] columnFamily) throws IOException {
  }

  @Override
  public void preDeleteColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] columnFamily) throws IOException {
  }

  /**
   * Called after the column family has been deleted.  Called as part of delete
   * column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #postDeleteColumnFamily(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] columnFamily) throws IOException {
  }

  @Override
  public void postDeleteColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] columnFamily) throws IOException {
  }

  /**
   * Called prior to deleting the entire column family.  Called as part of
   * delete column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preDeleteColumnFamilyAction(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  @Override
  public void preDeleteColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      byte[] columnFamily) throws IOException {
  }

  @Override
  public void preDeleteColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final byte[] columnFamily) throws IOException {
  }

  /**
   * Called after the column family has been deleted.  Called as part of
   * delete column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *         (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *         Use {@link #postCompletedDeleteColumnFamilyAction(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  @Override
  public void postDeleteColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      byte[] columnFamily) throws IOException {
  }

  @Override
  public void postCompletedDeleteColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final byte[] columnFamily) throws IOException {
  }


  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  /**
   * Called prior to enabling a table.  Called as part of enable table handler
   * and it is async to the enable table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preEnableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void preEnableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preEnableTableAction(
      ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {
  }

  /**
   * Called after the enableTable operation has been requested.  Called as part
   * of enable table handler and it is async to the enable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedEnableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void postEnableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postCompletedEnableTableAction(
      ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  /**
   * Called prior to disabling a table.  Called as part of disable table handler
   * and it is asyn to the disable table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preDisableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void preDisableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preDisableTableAction(
      ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {
  }

  /**
   * Called after the disableTable operation has been requested.  Called as part
   * of disable table handler and it is asyn to the disable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @throws IOException if something went wrong
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedDisableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  @Override
  public void postDisableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postCompletedDisableTableAction(
      ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {
  }

  @Override
  public void preAbortProcedure(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      final ProcedureExecutor<MasterProcedureEnv> procEnv,
      final long procId) throws IOException {
  }

  @Override
  public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postListProcedures(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ProcedureInfo> procInfoList) throws IOException {
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
    HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
    HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public boolean preSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
    return false;
  }

  @Override
  public void postSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
      throws IOException {
  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean b) throws IOException {
    return b;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean oldValue, boolean newValue) throws IOException {
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void preMasterInitialization(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
  }

  @Override
  public void stop(CoprocessorEnvironment ctx) throws IOException {
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo region, ServerName srcServer, ServerName destServer)
  throws IOException {
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo region, ServerName srcServer, ServerName destServer)
  throws IOException {
  }

  @Override
  public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void postListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex)
      throws IOException {
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
      String regex) throws IOException {
  }

  @Override
  public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors, String regex) throws IOException {
  }

  @Override
  public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors, String regex) throws IOException {
  }

  @Override
  public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort>
      servers, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort>
      servers, String targetGroup) throws IOException {
  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName>
      tables, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<TableName> tables, String targetGroup) throws IOException {
  }

  @Override
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {

  }

  @Override
  public void postRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName)
      throws IOException {
  }

  @Override
  public void postBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 String groupName, boolean balancerRan) throws IOException {
  }
}

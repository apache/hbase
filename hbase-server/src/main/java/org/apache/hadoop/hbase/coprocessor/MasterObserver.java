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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;


/**
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 * <br><br>
 *
 * Since most implementations will be interested in only a subset of hooks, this class uses
 * 'default' functions to avoid having to add unnecessary overrides. When the functions are
 * non-empty, it's simply to satisfy the compiler by returning value of expected (non-void) type.
 * It is done in a way that these default definitions act as no-op. So our suggestion to
 * implementation would be to not call these 'default' methods from overrides.
 * <br><br>
 *
 * <h3>Exception Handling</h3>
 * For all functions, exception handling is done as follows:
 * <ul>
 *   <li>Exceptions of type {@link IOException} are reported back to client.</li>
 *   <li>For any other kind of exception:
 *     <ul>
 *       <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then
 *         the server aborts.</li>
 *       <li>Otherwise, coprocessor is removed from the server and
 *         {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface MasterObserver extends Coprocessor {
  /**
   * Called before a new table is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.  Called as part of create
   * table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void preCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {}

  /**
   * Called after the createTable operation has been requested.  Called as part
   * of create table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void postCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {}

  /**
   * Called before a new table is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.  Called as part of create
   * table handler and it is async to the create RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preCreateTableAction(ObserverContext, HTableDescriptor, HRegionInfo[])}.
   */
  @Deprecated
  default void preCreateTableHandler(final ObserverContext<MasterCoprocessorEnvironment>
      ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {}

  /**
   * Called after the createTable operation has been requested.  Called as part
   * of create table RPC call.  Called as part of create table handler and
   * it is async to the create RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *   (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *   Use {@link #postCompletedCreateTableAction(ObserverContext, HTableDescriptor, HRegionInfo[])}
   */
  @Deprecated
  default void postCreateTableHandler(final ObserverContext<MasterCoprocessorEnvironment>
  ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {}

  /**
   * Called before a new table is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.  Called as part of create
   * table procedure and it is async to the create RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preCreateTableHandler(ObserverContext, HTableDescriptor, HRegionInfo[])} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void preCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HTableDescriptor desc,
      final HRegionInfo[] regions) throws IOException {}

  /**
   * Called after the createTable operation has been requested.  Called as part
   * of create table RPC call.  Called as part of create table procedure and
   * it is async to the create RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postCreateTableHandler(ObserverContext, HTableDescriptor, HRegionInfo[])} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void postCompletedCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HTableDescriptor desc,
      final HRegionInfo[] regions) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDeleteTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called after the deleteTable operation has been requested.  Called as part
   * of delete table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postDeleteTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table handler and
   * it is async to the delete RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preDeleteTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void preDeleteTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {}

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table handler and it is async to the
   * delete RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedDeleteTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void postDeleteTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table procedure and
   * it is async to the delete RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preDeleteTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table procedure and it is async to the
   * delete RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postDeleteTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called after the truncateTable operation has been requested.  Called as part
   * of truncate table RPC call.
   * The truncate is synchronous, so this method will be called when the
   * truncate operation is terminated.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table handler and it is sync
   * to the truncate RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preTruncateTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void preTruncateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {}

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table handler and it is sync to the
   * truncate RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedTruncateTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void postTruncateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table procedure and it is async
   * to the truncate RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preTruncateTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table procedure and it is async to the
   * truncate RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postTruncateTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   */
  default void preModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, HTableDescriptor htd) throws IOException {}

  /**
   * Called after the modifyTable operation has been requested.  Called as part
   * of modify table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   */
  default void postModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, HTableDescriptor htd) throws IOException {}

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table handler and it is async to the modify table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preModifyTableAction(ObserverContext, TableName, HTableDescriptor)}.
   */
  @Deprecated
  default void preModifyTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, HTableDescriptor htd) throws IOException {}

  /**
   * Called after to modifying a table's properties.  Called as part of modify
   * table handler and it is async to the modify table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *     Use {@link #postCompletedModifyTableAction(ObserverContext, TableName, HTableDescriptor)}.
   */
  @Deprecated
  default void postModifyTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, HTableDescriptor htd) throws IOException {}

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table procedure and it is async to the modify table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preModifyTableHandler(ObserverContext, TableName, HTableDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   */
  default void preModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HTableDescriptor htd) throws IOException {}

  /**
   * Called after to modifying a table's properties.  Called as part of modify
   * table procedure and it is async to the modify table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postModifyTableHandler(ObserverContext, TableName, HTableDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   */
  default void postCompletedModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HTableDescriptor htd) throws IOException {}

  /**
   * Called prior to adding a new column family to the table.  Called as part of
   * add column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preAddColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  default void preAddColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to adding a new column family to the table.  Called as part of
   * add column RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preAddColumn(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void preAddColumnFamily(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the new column family has been created.  Called as part of
   * add column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #postAddColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  default void postAddColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the new column family has been created.  Called as part of
   * add column RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postAddColumn(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void postAddColumnFamily(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to adding a new column family to the table.  Called as part of
   * add column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *          (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>). Use
   *          {@link #preAddColumnFamilyAction(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  default void preAddColumnHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to adding a new column family to the table.  Called as part of
   * add column procedure.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preAddColumnHandler(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void preAddColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the new column family has been created.  Called as part of
   * add column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>). Use
   *     {@link #postCompletedAddColumnFamilyAction(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  default void postAddColumnHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the new column family has been created.  Called as part of
   * add column procedure.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postAddColumnHandler(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void postCompletedAddColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to modifying a column family's attributes.  Called as part of
   * modify column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preModifyColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  default void preModifyColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to modifying a column family's attributes.  Called as part of
   * modify column RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preModifyColumn(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void preModifyColumnFamily(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the column family has been updated.  Called as part of modify
   * column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #postModifyColumnFamily(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  default void postModifyColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the column family has been updated.  Called as part of modify
   * column RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postModifyColumn(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void postModifyColumnFamily(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to modifying a column family's attributes.  Called as part of
   * modify column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *     Use {@link #preModifyColumnFamilyAction(ObserverContext, TableName, HColumnDescriptor)}.
   */
  @Deprecated
  default void preModifyColumnHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to modifying a column family's attributes.  Called as part of
   * modify column procedure.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preModifyColumnHandler(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void preModifyColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the column family has been updated.  Called as part of modify
   * column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *   (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>). Use
   *   {@link #postCompletedModifyColumnFamilyAction(ObserverContext,TableName,HColumnDescriptor)}.
   */
  @Deprecated
  default void postModifyColumnHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called after the column family has been updated.  Called as part of modify
   * column procedure.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postModifyColumnHandler(ObserverContext, TableName, HColumnDescriptor)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the HColumnDescriptor
   */
  default void postCompletedModifyColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {}

  /**
   * Called prior to deleting the entire column family.  Called as part of
   * delete column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preDeleteColumnFamily(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  default void preDeleteColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called prior to deleting the entire column family.  Called as part of
   * delete column RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preDeleteColumn(ObserverContext, TableName, byte[])} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column
   */
  default void preDeleteColumnFamily(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called after the column family has been deleted.  Called as part of delete
   * column RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #postDeleteColumnFamily(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  default void postDeleteColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called after the column family has been deleted.  Called as part of delete
   * column RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postDeleteColumn(ObserverContext, TableName, byte[])} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   */
  default void postDeleteColumnFamily(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called prior to deleting the entire column family.  Called as part of
   * delete column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *             Use {@link #preDeleteColumnFamilyAction(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  default void preDeleteColumnHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called prior to deleting the entire column family.  Called as part of
   * delete column procedure.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preDeleteColumnHandler(ObserverContext, TableName, byte[])} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   */
  default void preDeleteColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called after the column family has been deleted.  Called as part of
   * delete column handler.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *         (<a href="https://issues.apache.org/jira/browse/HBASE-13645">HBASE-13645</a>).
   *         Use {@link #postCompletedDeleteColumnFamilyAction(ObserverContext, TableName, byte[])}.
   */
  @Deprecated
  default void postDeleteColumnHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called after the column family has been deleted.  Called as part of
   * delete column procedure.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postDeleteColumnHandler(ObserverContext, TableName, byte[])} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param columnFamily the column family
   */
  default void postCompletedDeleteColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {}

  /**
   * Called prior to enabling a table.  Called as part of enable table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preEnableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the enableTable operation has been requested.  Called as part
   * of enable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postEnableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to enabling a table.  Called as part of enable table handler
   * and it is async to the enable table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preEnableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void preEnableTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the enableTable operation has been requested.  Called as part
   * of enable table handler and it is async to the enable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedEnableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void postEnableTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to enabling a table.  Called as part of enable table procedure
   * and it is async to the enable table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preEnableTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preEnableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the enableTable operation has been requested.  Called as part
   * of enable table procedure and it is async to the enable table RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postEnableTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedEnableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to disabling a table.  Called as part of disable table RPC
   * call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDisableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the disableTable operation has been requested.  Called as part
   * of disable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postDisableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to disabling a table.  Called as part of disable table handler
   * and it is asyn to the disable table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #preDisableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void preDisableTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the disableTable operation has been requested.  Called as part
   * of disable table handler and it is asyn to the disable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     (<a href="https://issues.apache.org/jira/browse/HBASE-15575">HBASE-15575</a>).
   *     Use {@link #postCompletedDisableTableAction(ObserverContext, TableName)}.
   */
  @Deprecated
  default void postDisableTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to disabling a table.  Called as part of disable table procedure
   * and it is asyn to the disable table RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   *
   * Implementation note: This replaces the deprecated
   * {@link #preDisableTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDisableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the disableTable operation has been requested.  Called as part
   * of disable table procedure and it is asyn to the disable table RPC call.
   *
   * Implementation note: This replaces the deprecated
   * {@link #postDisableTableHandler(ObserverContext, TableName)} method.
   * Make sure to implement only one of the two as both are called.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedDisableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called before a abortProcedure request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param procEnv procedure executor
   * @param procId the Id of the procedure
   */
  default void preAbortProcedure(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      final ProcedureExecutor<MasterProcedureEnv> procEnv,
      final long procId) throws IOException {}

  /**
   * Called after a abortProcedure request has been processed.
   * @param ctx the environment to interact with the framework and master
   */
  default void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before a listProcedures request has been processed.
   * @param ctx the environment to interact with the framework and master
   */
  default void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after a listProcedures request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param procInfoList the list of procedures about to be returned
   */
  default void postListProcedures(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ProcedureInfo> procInfoList) throws IOException {}

  /**
   * Called prior to moving a given region from one region server to another.
   * @param ctx the environment to interact with the framework and master
   * @param region the HRegionInfo
   * @param srcServer the source ServerName
   * @param destServer the destination ServerName
   */
  default void preMove(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer)
    throws IOException {}

  /**
   * Called after the region move has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param region the HRegionInfo
   * @param srcServer the source ServerName
   * @param destServer the destination ServerName
   */
  default void postMove(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer)
    throws IOException {}

  /**
   * Called prior to assigning a specific region.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo the regionInfo of the region
   */
  default void preAssign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo) throws IOException {}

  /**
   * Called after the region assignment has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo the regionInfo of the region
   */
  default void postAssign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo) throws IOException {}

  /**
   * Called prior to unassigning a given region.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   * @param force whether to force unassignment or not
   */
  default void preUnassign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo, final boolean force) throws IOException {}

  /**
   * Called after the region unassignment has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   * @param force whether to force unassignment or not
   */
  default void postUnassign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo, final boolean force) throws IOException {}

  /**
   * Called prior to marking a given region as offline. <code>ctx.bypass()</code> will not have any
   * impact on this hook.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   */
  default void preRegionOffline(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo) throws IOException {}

  /**
   * Called after the region has been marked offline.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   */
  default void postRegionOffline(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo) throws IOException {}

  /**
   * Called prior to requesting rebalancing of the cluster regions, though after
   * the initial checks for regions in transition and the balance switch flag.
   * @param ctx the environment to interact with the framework and master
   */
  default void preBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after the balancing plan has been submitted.
   * @param ctx the environment to interact with the framework and master
   * @param plans the RegionPlans which master has executed. RegionPlan serves as hint
   * as for the final destination for the underlying region but may not represent the
   * final state of assignment
   */
  default void postBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
      throws IOException {}

  /**
   * Called prior to setting split / merge switch
   * @param ctx the coprocessor instance's environment
   * @param newValue the new value submitted in the call
   * @param switchType type of switch
   */
  default boolean preSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
    return false;
  }

  /**
   * Called after setting split / merge switch
   * @param ctx the coprocessor instance's environment
   * @param newValue the new value submitted in the call
   * @param switchType type of switch
   */
  default void postSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {}

  /**
   * Called before the split region procedure is called.
   * @param c the environment to interact with the framework and master
   * @param tableName the table where the region belongs to
   * @param splitRow split point
   */
  default void preSplitRegion(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName,
      final byte[] splitRow)
      throws IOException {}

  /**
   * Called before the region is split.
   * @param c the environment to interact with the framework and master
   * @param tableName the table where the region belongs to
   * @param splitRow split point
   */
  default void preSplitRegionAction(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName,
      final byte[] splitRow)
      throws IOException {}

  /**
   * Called after the region is split.
   * @param c the environment to interact with the framework and master
   * @param regionInfoA the left daughter region
   * @param regionInfoB the right daughter region
   */
  default void postCompletedSplitRegionAction(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final HRegionInfo regionInfoA,
      final HRegionInfo regionInfoB) throws IOException {}

  /**
   * This will be called before PONR step as part of split transaction. Calling
   * {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} rollback the split
   * @param ctx the environment to interact with the framework and master
   * @param splitKey
   * @param metaEntries
   */
  default void preSplitRegionBeforePONRAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] splitKey,
      final List<Mutation> metaEntries) throws IOException {}


  /**
   * This will be called after PONR step as part of split transaction
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * @param ctx the environment to interact with the framework and master
   */
  default void preSplitRegionAfterPONRAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * This will be called after the roll back of the split region is completed
   * @param ctx the environment to interact with the framework and master
   */
  default void postRollBackSplitRegionAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before the regions merge.
   * Call {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} to skip the merge.
   * @param ctx the environment to interact with the framework and master
   */
  default void preMergeRegionsAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo[] regionsToMerge) throws IOException {}

  /**
   * called after the regions merge.
   * @param ctx the environment to interact with the framework and master
   */
  default void postCompletedMergeRegionsAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo[] regionsToMerge,
      final HRegionInfo mergedRegion) throws IOException {}

  /**
   * This will be called before PONR step as part of regions merge transaction. Calling
   * {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} rollback the merge
   * @param ctx the environment to interact with the framework and master
   * @param metaEntries mutations to execute on hbase:meta atomically with regions merge updates.
   *        Any puts or deletes to execute on hbase:meta can be added to the mutations.
   */
  default void preMergeRegionsCommitAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo[] regionsToMerge,
      @MetaMutationAnnotation List<Mutation> metaEntries) throws IOException {}

  /**
   * This will be called after PONR step as part of regions merge transaction.
   * @param ctx the environment to interact with the framework and master
   */
  default void postMergeRegionsCommitAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo[] regionsToMerge,
      final HRegionInfo mergedRegion) throws IOException {}

  /**
   * This will be called after the roll back of the regions merge.
   * @param ctx the environment to interact with the framework and master
   */
  default void postRollBackMergeRegionsAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo[] regionsToMerge) throws IOException {}

  /**
   * Called prior to modifying the flag used to enable/disable region balancing.
   * @param ctx the coprocessor instance's environment
   * @param newValue the new flag value submitted in the call
   */
  default boolean preBalanceSwitch(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue) throws IOException {
    return newValue;
  }

  /**
   * Called after the flag to enable/disable balancing has changed.
   * @param ctx the coprocessor instance's environment
   * @param oldValue the previously set balanceSwitch value
   * @param newValue the newly set balanceSwitch value
   */
  default void postBalanceSwitch(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean oldValue, final boolean newValue) throws IOException {}

  /**
   * Called prior to shutting down the full HBase cluster, including this
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  default void preShutdown(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}


  /**
   * Called immediately prior to stopping this
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  default void preStopMaster(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called immediately after an active master instance has completed
   * initialization.  Will not be called on standby master instances unless
   * they take over the active role.
   */
  default void postStartMaster(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Call before the master initialization is set to true.
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  default void preMasterInitialization(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before a new snapshot is taken.
   * Called as part of snapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to snapshot
   */
  default void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {}

  /**
   * Called after the snapshot operation has been requested.
   * Called as part of snapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to snapshot
   */
  default void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {}

  /**
   * Called before listSnapshots request has been processed.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to list
   */
  default void preListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called after listSnapshots request has been processed.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to list
   */
  default void postListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called before a snapshot is cloned.
   * Called as part of restoreSnapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to create
   */
  default void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {}

  /**
   * Called after a snapshot clone operation has been requested.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to create
   */
  default void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {}

  /**
   * Called before a snapshot is restored.
   * Called as part of restoreSnapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to restore
   */
  default void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {}

  /**
   * Called after a snapshot restore operation has been requested.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to restore
   */
  default void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {}

  /**
   * Called before a snapshot is deleted.
   * Called as part of deleteSnapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to delete
   */
  default void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called after the delete snapshot operation has been requested.
   * Called as part of deleteSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to delete
   */
  default void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called before a getTableDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param tableNamesList the list of table names, or null if querying for all
   * @param descriptors an empty list, can be filled with what to return if bypassing
   * @param regex regular expression used for filtering the table names
   */
  default void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
      String regex) throws IOException {}

  /**
   * Called after a getTableDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param tableNamesList the list of table names, or null if querying for all
   * @param descriptors the list of descriptors about to be returned
   * @param regex regular expression used for filtering the table names
   */
  default void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
      String regex) throws IOException {}

  /**
   * Called before a getTableNames request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors an empty list, can be filled with what to return if bypassing
   * @param regex regular expression used for filtering the table names
   */
  default void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors, String regex) throws IOException {}

  /**
   * Called after a getTableNames request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors the list of descriptors about to be returned
   * @param regex regular expression used for filtering the table names
   */
  default void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors, String regex) throws IOException {}



  /**
   * Called before a new namespace is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor for the table
   */
  default void preCreateNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {}
  /**
   * Called after the createNamespace operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor for the table
   */
  default void postCreateNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
       NamespaceDescriptor ns) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * namespace
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   */
  default void preDeleteNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {}

  /**
   * Called after the deleteNamespace operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   */
  default void postDeleteNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {}

  /**
   * Called prior to modifying a namespace's properties.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor
   */
  default void preModifyNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {}

  /**
   * Called after the modifyNamespace operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor
   */
  default void postModifyNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {}

  /**
   * Called before a getNamespaceDescriptor request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   */
  default void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {}

  /**
   * Called after a getNamespaceDescriptor request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor
   */
  default void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {}

  /**
   * Called before a listNamespaceDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors an empty list, can be filled with what to return if bypassing
   */
  default void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {}

  /**
   * Called after a listNamespaceDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors the list of descriptors about to be returned
   */
  default void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {}


  /**
   * Called before the table memstore is flushed to disk.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the table memstore is flushed to disk.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called before the quota for the user is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param quotas the quota settings
   */
  default void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {}

  /**
   * Called after the quota for the user is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param quotas the quota settings
   */
  default void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {}

  /**
   * Called before the quota for the user on the specified table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param tableName the name of the table
   * @param quotas the quota settings
   */
  default void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {}

  /**
   * Called after the quota for the user on the specified table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param tableName the name of the table
   * @param quotas the quota settings
   */
  default void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {}

  /**
   * Called before the quota for the user on the specified namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param namespace the name of the namespace
   * @param quotas the quota settings
   */
  default void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {}

  /**
   * Called after the quota for the user on the specified namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param namespace the name of the namespace
   * @param quotas the quota settings
   */
  default void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {}

  /**
   * Called before the quota for the table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param quotas the quota settings
   */
  default void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {}

  /**
   * Called after the quota for the table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param quotas the quota settings
   */
  default void postSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {}

  /**
   * Called before the quota for the namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   * @param quotas the quota settings
   */
  default void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {}

  /**
   * Called after the quota for the namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   * @param quotas the quota settings
   */
  default void postSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {}

  /**
   * Called before dispatching region merge request.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx coprocessor environment
   * @param regionA first region to be merged
   * @param regionB second region to be merged
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *   (<a href="https://issues.apache.org/jira/browse/HBASE-">HBASE-</a>).
   *   Use {@link #preMergeRegions(ObserverContext, HRegionInfo[])}
   */
  @Deprecated
  default void preDispatchMerge(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionA, HRegionInfo regionB) throws IOException {}

  /**
   * called after dispatching the region merge request.
   * @param c coprocessor environment
   * @param regionA first region to be merged
   * @param regionB second region to be merged
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *   (<a href="https://issues.apache.org/jira/browse/HBASE-">HBASE-</a>).
   *   Use {@link #postMergeRegions(ObserverContext, HRegionInfo[])}
   */
  @Deprecated
  default void postDispatchMerge(final ObserverContext<MasterCoprocessorEnvironment> c,
      final HRegionInfo regionA, final HRegionInfo regionB) throws IOException {}

  /**
   * Called before merge regions request.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx coprocessor environment
   * @param regionsToMerge regions to be merged
   */
  default void preMergeRegions(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo[] regionsToMerge) throws IOException {}

  /**
   * called after merge regions request.
   * @param c coprocessor environment
   * @param regionsToMerge regions to be merged
   */
  default void postMergeRegions(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final HRegionInfo[] regionsToMerge) throws IOException {}

  /**
   * Called before servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup destination group
   */
  default void preMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called after servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup name of group
   */
  default void postMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called before servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup destination group
   */
  default void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      Set<Address> servers, String targetGroup) throws IOException {}

  /**
   * Called after servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup name of group
   */
  default void postMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                       Set<Address> servers, String targetGroup) throws IOException {}

  /**
   * Called before tables are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param tables set of tables to move
   * @param targetGroup name of group
   */
  default void preMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                     Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called after servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param tables set of tables to move
   * @param targetGroup name of group
   */
  default void postMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called before a new region server group is added
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void preAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                     String name) throws IOException {}

  /**
   * Called after a new region server group is added
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void postAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      String name) throws IOException {}

  /**
   * Called before a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void preRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                        String name) throws IOException {}

  /**
   * Called after a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void postRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                         String name) throws IOException {}

  /**
   * Called before a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param groupName group name
   */
  default void preBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                         String groupName) throws IOException {}

  /**
   * Called after a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param groupName group name
   */
  default void postBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                          String groupName, boolean balancerRan) throws IOException {}

  /**
   * Called before add a replication peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   */
  default void preAddReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId, ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called after add a replication peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   */
  default void postAddReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId, ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called before remove a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preRemoveReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after remove a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postRemoveReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called before enable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preEnableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after enable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postEnableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called before disable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preDisableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after disable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postDisableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called before get the configured ReplicationPeerConfig for the specified peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preGetReplicationPeerConfig(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after get the configured ReplicationPeerConfig for the specified peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postGetReplicationPeerConfig(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId) throws IOException {}

  /**
   * Called before update peerConfig for the specified peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preUpdateReplicationPeerConfig(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId,
      ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called after update peerConfig for the specified peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   */
  default void postUpdateReplicationPeerConfig(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId,
      ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called before list replication peers.
   * @param ctx the environment to interact with the framework and master
   * @param regex The regular expression to match peer id
   */
  default void preListReplicationPeers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String regex) throws IOException {}

  /**
   * Called after list replication peers.
   * @param ctx the environment to interact with the framework and master
   * @param regex The regular expression to match peer id
   */
  default void postListReplicationPeers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String regex) throws IOException {}

  /**
   * Called before new LockProcedure is queued.
   * @param ctx the environment to interact with the framework and master
   */
  default void preRequestLock(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace,
      TableName tableName, HRegionInfo[] regionInfos, LockProcedure.LockType type,
      String description) throws IOException {}

  /**
   * Called after new LockProcedure is queued.
   * @param ctx the environment to interact with the framework and master
   */
  default void postRequestLock(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace,
      TableName tableName, HRegionInfo[] regionInfos, LockProcedure.LockType type,
      String description) throws IOException {}

  /**
   * Called before heartbeat to a lock.
   * @param ctx the environment to interact with the framework and master
   * @param keepAlive if lock should be kept alive; lock will be released if set to false.
   */
  default void preLockHeartbeat(ObserverContext<MasterCoprocessorEnvironment> ctx,
      LockProcedure proc, boolean keepAlive) throws IOException {}

  /**
   * Called after heartbeat to a lock.
   * @param ctx the environment to interact with the framework and master
   * @param keepAlive if lock was kept alive; lock was released if set to false.
   */
  default void postLockHeartbeat(ObserverContext<MasterCoprocessorEnvironment> ctx,
      LockProcedure proc, boolean keepAlive) throws IOException {}
}

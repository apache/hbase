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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CreateNamespaceState;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * The procedure to create a new namespace.
 */
@InterfaceAudience.Private
public class CreateNamespaceProcedure
    extends AbstractStateMachineNamespaceProcedure<CreateNamespaceState> {
  private static final Log LOG = LogFactory.getLog(CreateNamespaceProcedure.class);

  private NamespaceDescriptor nsDescriptor;
  private Boolean traceEnabled;

  public CreateNamespaceProcedure() {
    this.traceEnabled = null;
  }

  public CreateNamespaceProcedure(final MasterProcedureEnv env,
      final NamespaceDescriptor nsDescriptor) {
    super(env);
    this.nsDescriptor = nsDescriptor;
    this.traceEnabled = null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final CreateNamespaceState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case CREATE_NAMESPACE_PREPARE:
        prepareCreate(env);
        setNextState(CreateNamespaceState.CREATE_NAMESPACE_CREATE_DIRECTORY);
        break;
      case CREATE_NAMESPACE_CREATE_DIRECTORY:
        createDirectory(env, nsDescriptor);
        setNextState(CreateNamespaceState.CREATE_NAMESPACE_INSERT_INTO_NS_TABLE);
        break;
      case CREATE_NAMESPACE_INSERT_INTO_NS_TABLE:
        insertIntoNSTable(env, nsDescriptor);
        setNextState(CreateNamespaceState.CREATE_NAMESPACE_UPDATE_ZK);
        break;
      case CREATE_NAMESPACE_UPDATE_ZK:
        updateZKNamespaceManager(env, nsDescriptor);
        setNextState(CreateNamespaceState.CREATE_NAMESPACE_SET_NAMESPACE_QUOTA);
        break;
      case CREATE_NAMESPACE_SET_NAMESPACE_QUOTA:
        setNamespaceQuota(env, nsDescriptor);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-create-namespace", e);
      } else {
        LOG.warn("Retriable error trying to create namespace=" + nsDescriptor.getName() +
          " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final CreateNamespaceState state)
      throws IOException {
    if (state == CreateNamespaceState.CREATE_NAMESPACE_PREPARE) {
      // nothing to rollback, pre-create is just state checks.
      // TODO: coprocessor rollback semantic is still undefined.
      return;
    }
    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final CreateNamespaceState state) {
    switch (state) {
      case CREATE_NAMESPACE_PREPARE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected CreateNamespaceState getState(final int stateId) {
    return CreateNamespaceState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final CreateNamespaceState state) {
    return state.getNumber();
  }

  @Override
  protected CreateNamespaceState getInitialState() {
    return CreateNamespaceState.CREATE_NAMESPACE_PREPARE;
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.CreateNamespaceStateData.Builder createNamespaceMsg =
        MasterProcedureProtos.CreateNamespaceStateData.newBuilder().setNamespaceDescriptor(
          ProtobufUtil.toProtoNamespaceDescriptor(this.nsDescriptor));
    createNamespaceMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.CreateNamespaceStateData createNamespaceMsg =
        MasterProcedureProtos.CreateNamespaceStateData.parseDelimitedFrom(stream);
    nsDescriptor = ProtobufUtil.toNamespaceDescriptor(createNamespaceMsg.getNamespaceDescriptor());
  }

  private boolean isBootstrapNamespace() {
    return nsDescriptor.equals(NamespaceDescriptor.DEFAULT_NAMESPACE) ||
        nsDescriptor.equals(NamespaceDescriptor.SYSTEM_NAMESPACE);
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (!env.getMasterServices().isInitialized()) {
      // Namespace manager might not be ready if master is not fully initialized,
      // return false to reject user namespace creation; return true for default
      // and system namespace creation (this is part of master initialization).
      if (!isBootstrapNamespace() && env.waitInitialized(this)) {
        return false;
      }
    }
    return env.getProcedureQueue().tryAcquireNamespaceExclusiveLock(this, getNamespaceName());
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  @Override
  protected String getNamespaceName() {
    return nsDescriptor.getName();
  }

  /**
   * Action before any real action of creating namespace.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareCreate(final MasterProcedureEnv env) throws IOException {
    if (getTableNamespaceManager(env).doesNamespaceExist(nsDescriptor.getName())) {
      throw new NamespaceExistException(nsDescriptor.getName());
    }
    getTableNamespaceManager(env).validateTableAndRegionCount(nsDescriptor);
  }

  /**
   * Create the namespace directory
   * @param env MasterProcedureEnv
   * @param nsDescriptor NamespaceDescriptor
   * @throws IOException
   */
  protected static void createDirectory(
      final MasterProcedureEnv env,
      final NamespaceDescriptor nsDescriptor) throws IOException {
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    mfs.getFileSystem().mkdirs(
      FSUtils.getNamespaceDir(mfs.getRootDir(), nsDescriptor.getName()));
  }

  /**
   * Insert the row into ns table
   * @param env MasterProcedureEnv
   * @param nsDescriptor NamespaceDescriptor
   * @throws IOException
   */
  protected static void insertIntoNSTable(
      final MasterProcedureEnv env,
      final NamespaceDescriptor nsDescriptor) throws IOException {
    getTableNamespaceManager(env).insertIntoNSTable(nsDescriptor);
  }

  /**
   * Update ZooKeeper.
   * @param env MasterProcedureEnv
   * @param nsDescriptor NamespaceDescriptor
   * @throws IOException
   */
  protected static void updateZKNamespaceManager(
      final MasterProcedureEnv env,
      final NamespaceDescriptor nsDescriptor) throws IOException {
    getTableNamespaceManager(env).updateZKNamespaceManager(nsDescriptor);
  }

  /**
   * Set quota for the namespace
   * @param env MasterProcedureEnv
   * @param nsDescriptor NamespaceDescriptor
   * @throws IOException
   **/
  protected static void setNamespaceQuota(
      final MasterProcedureEnv env,
      final NamespaceDescriptor nsDescriptor) throws IOException {
    if (env.getMasterServices().isInitialized()) {
      env.getMasterServices().getMasterQuotaManager().setNamespaceQuota(nsDescriptor);
    }
  }

  /**
   * remove quota for the namespace if exists
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void rollbackSetNamespaceQuota(final MasterProcedureEnv env) throws IOException {
    try {
      DeleteNamespaceProcedure.removeNamespaceQuota(env, nsDescriptor.getName());
    } catch (Exception e) {
      // Ignore exception
      LOG.debug("Rollback of setNamespaceQuota throws exception: " + e);
    }
  }

  private static TableNamespaceManager getTableNamespaceManager(final MasterProcedureEnv env) {
    return env.getMasterServices().getClusterSchema().getTableNamespaceManager();
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return traceEnabled
   */
  private Boolean isTraceEnabled() {
    if (traceEnabled == null) {
      traceEnabled = LOG.isTraceEnabled();
    }
    return traceEnabled;
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
    // hbase and default namespaces are created on bootstrap internally by the system
    // the client does not know about this procedures.
    return !isBootstrapNamespace();
  }
}

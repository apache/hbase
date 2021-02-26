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
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CreateNamespaceState;

/**
 * The procedure to create a new namespace.
 */
@InterfaceAudience.Private
public class CreateNamespaceProcedure
    extends AbstractStateMachineNamespaceProcedure<CreateNamespaceState> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateNamespaceProcedure.class);

  private NamespaceDescriptor nsDescriptor;
  private Boolean traceEnabled;

  public CreateNamespaceProcedure() {
    this.traceEnabled = null;
  }

  public CreateNamespaceProcedure(final MasterProcedureEnv env,
      final NamespaceDescriptor nsDescriptor) {
    this(env, nsDescriptor, null);
  }

  public CreateNamespaceProcedure(final MasterProcedureEnv env,
      final NamespaceDescriptor nsDescriptor, ProcedurePrepareLatch latch) {
    super(env, latch);
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
        boolean success = prepareCreate(env);
        releaseSyncLatch();
        if (!success) {
          assert isFailed() : "createNamespace should have an exception here";
          return Flow.NO_MORE_STATE;
        }
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
      releaseSyncLatch();
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
    return CreateNamespaceState.forNumber(stateId);
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
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.CreateNamespaceStateData.Builder createNamespaceMsg =
        MasterProcedureProtos.CreateNamespaceStateData.newBuilder().setNamespaceDescriptor(
          ProtobufUtil.toProtoNamespaceDescriptor(this.nsDescriptor));
    serializer.serialize(createNamespaceMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.CreateNamespaceStateData createNamespaceMsg =
        serializer.deserialize(MasterProcedureProtos.CreateNamespaceStateData.class);
    nsDescriptor = ProtobufUtil.toNamespaceDescriptor(createNamespaceMsg.getNamespaceDescriptor());
  }

  private boolean isBootstrapNamespace() {
    return nsDescriptor.equals(NamespaceDescriptor.DEFAULT_NAMESPACE) ||
        nsDescriptor.equals(NamespaceDescriptor.SYSTEM_NAMESPACE);
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    // Namespace manager might not be ready if master is not fully initialized,
    // return false to reject user namespace creation; return true for default
    // and system namespace creation (this is part of master initialization).
    if (isBootstrapNamespace()) {
      return false;
    }
    return env.waitInitialized(this);
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitNamespaceExclusiveLock(this, getNamespaceName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
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
  private boolean prepareCreate(final MasterProcedureEnv env) throws IOException {
    if (getTableNamespaceManager(env).doesNamespaceExist(nsDescriptor.getName())) {
      setFailure("master-create-namespace",
          new NamespaceExistException("Namespace " + nsDescriptor.getName() + " already exists"));
      return false;
    }
    getTableNamespaceManager(env).validateTableAndRegionCount(nsDescriptor);
    return true;
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
      CommonFSUtils.getNamespaceDir(mfs.getRootDir(), nsDescriptor.getName()));
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

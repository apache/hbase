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
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyNamespaceState;

/**
 * The procedure to add a namespace to an existing table.
 */
@InterfaceAudience.Private
public class ModifyNamespaceProcedure
    extends AbstractStateMachineNamespaceProcedure<ModifyNamespaceState> {
  private static final Logger LOG = LoggerFactory.getLogger(ModifyNamespaceProcedure.class);

  private NamespaceDescriptor oldNsDescriptor;
  private NamespaceDescriptor newNsDescriptor;
  private Boolean traceEnabled;

  public ModifyNamespaceProcedure() {
    this.oldNsDescriptor = null;
    this.traceEnabled = null;
  }

  public ModifyNamespaceProcedure(final MasterProcedureEnv env,
      final NamespaceDescriptor newNsDescriptor) {
    this(env, newNsDescriptor, null);
  }

  public ModifyNamespaceProcedure(final MasterProcedureEnv env,
      final NamespaceDescriptor newNsDescriptor, final ProcedurePrepareLatch latch) {
    super(env, latch);
    this.oldNsDescriptor = null;
    this.newNsDescriptor = newNsDescriptor;
    this.traceEnabled = null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final ModifyNamespaceState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case MODIFY_NAMESPACE_PREPARE:
        boolean success = prepareModify(env);
        releaseSyncLatch();
        if (!success) {
          assert isFailed() : "Modify namespace should have an exception here";
          return Flow.NO_MORE_STATE;
        }
        setNextState(ModifyNamespaceState.MODIFY_NAMESPACE_UPDATE_NS_TABLE);
        break;
      case MODIFY_NAMESPACE_UPDATE_NS_TABLE:
        insertIntoNSTable(env);
        setNextState(ModifyNamespaceState.MODIFY_NAMESPACE_UPDATE_ZK);
        break;
      case MODIFY_NAMESPACE_UPDATE_ZK:
        updateZKNamespaceManager(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-modify-namespace", e);
      } else {
        LOG.warn("Retriable error trying to modify namespace=" + newNsDescriptor.getName() +
          " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final ModifyNamespaceState state)
      throws IOException {
    if (state == ModifyNamespaceState.MODIFY_NAMESPACE_PREPARE) {
      // nothing to rollback, pre-modify is just checks.
      // TODO: coprocessor rollback semantic is still undefined.
      releaseSyncLatch();
      return;
    }

    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final ModifyNamespaceState state) {
    switch (state) {
      case MODIFY_NAMESPACE_PREPARE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected ModifyNamespaceState getState(final int stateId) {
    return ModifyNamespaceState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final ModifyNamespaceState state) {
    return state.getNumber();
  }

  @Override
  protected ModifyNamespaceState getInitialState() {
    return ModifyNamespaceState.MODIFY_NAMESPACE_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.ModifyNamespaceStateData.Builder modifyNamespaceMsg =
        MasterProcedureProtos.ModifyNamespaceStateData.newBuilder().setNamespaceDescriptor(
          ProtobufUtil.toProtoNamespaceDescriptor(this.newNsDescriptor));
    if (this.oldNsDescriptor != null) {
      modifyNamespaceMsg.setUnmodifiedNamespaceDescriptor(
        ProtobufUtil.toProtoNamespaceDescriptor(this.oldNsDescriptor));
    }
    serializer.serialize(modifyNamespaceMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.ModifyNamespaceStateData modifyNamespaceMsg =
        serializer.deserialize(MasterProcedureProtos.ModifyNamespaceStateData.class);
    newNsDescriptor =
        ProtobufUtil.toNamespaceDescriptor(modifyNamespaceMsg.getNamespaceDescriptor());
    if (modifyNamespaceMsg.hasUnmodifiedNamespaceDescriptor()) {
      oldNsDescriptor =
          ProtobufUtil.toNamespaceDescriptor(modifyNamespaceMsg.getUnmodifiedNamespaceDescriptor());
    }
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  @Override
  protected String getNamespaceName() {
    return newNsDescriptor.getName();
  }

  /**
   * Action before any real action of adding namespace.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private boolean prepareModify(final MasterProcedureEnv env) throws IOException {
    if (getTableNamespaceManager(env).doesNamespaceExist(newNsDescriptor.getName()) == false) {
      setFailure("master-modify-namespace", new NamespaceNotFoundException(
            newNsDescriptor.getName()));
      return false;
    }
    try {
      getTableNamespaceManager(env).validateTableAndRegionCount(newNsDescriptor);
    } catch (ConstraintException e) {
      setFailure("master-modify-namespace", e);
      return false;
    }

    // This is used for rollback
    oldNsDescriptor = getTableNamespaceManager(env).get(newNsDescriptor.getName());
    return true;
  }

  /**
   * Insert/update the row into namespace table
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void insertIntoNSTable(final MasterProcedureEnv env) throws IOException {
    getTableNamespaceManager(env).insertIntoNSTable(newNsDescriptor);
  }

  /**
   * Update ZooKeeper.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void updateZKNamespaceManager(final MasterProcedureEnv env) throws IOException {
    getTableNamespaceManager(env).updateZKNamespaceManager(newNsDescriptor);
  }

  private TableNamespaceManager getTableNamespaceManager(final MasterProcedureEnv env) {
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
}

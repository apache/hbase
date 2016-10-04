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
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyNamespaceState;

/**
 * The procedure to add a namespace to an existing table.
 */
@InterfaceAudience.Private
public class ModifyNamespaceProcedure
    extends AbstractStateMachineNamespaceProcedure<ModifyNamespaceState> {
  private static final Log LOG = LogFactory.getLog(ModifyNamespaceProcedure.class);

  private NamespaceDescriptor oldNsDescriptor;
  private NamespaceDescriptor newNsDescriptor;
  private Boolean traceEnabled;

  public ModifyNamespaceProcedure() {
    this.oldNsDescriptor = null;
    this.traceEnabled = null;
  }

  public ModifyNamespaceProcedure(final MasterProcedureEnv env,
      final NamespaceDescriptor newNsDescriptor) {
    super(env);
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
        prepareModify(env);
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
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.ModifyNamespaceStateData.Builder modifyNamespaceMsg =
        MasterProcedureProtos.ModifyNamespaceStateData.newBuilder().setNamespaceDescriptor(
          ProtobufUtil.toProtoNamespaceDescriptor(this.newNsDescriptor));
    if (this.oldNsDescriptor != null) {
      modifyNamespaceMsg.setUnmodifiedNamespaceDescriptor(
        ProtobufUtil.toProtoNamespaceDescriptor(this.oldNsDescriptor));
    }
    modifyNamespaceMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.ModifyNamespaceStateData modifyNamespaceMsg =
        MasterProcedureProtos.ModifyNamespaceStateData.parseDelimitedFrom(stream);
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
  private void prepareModify(final MasterProcedureEnv env) throws IOException {
    if (getTableNamespaceManager(env).doesNamespaceExist(newNsDescriptor.getName()) == false) {
      throw new NamespaceNotFoundException(newNsDescriptor.getName());
    }
    getTableNamespaceManager(env).validateTableAndRegionCount(newNsDescriptor);

    // This is used for rollback
    oldNsDescriptor = getTableNamespaceManager(env).get(newNsDescriptor.getName());
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

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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.AddColumnFamilyState;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * The procedure to add a column family to an existing table.
 */
@InterfaceAudience.Private
public class AddColumnFamilyProcedure
    extends StateMachineProcedure<MasterProcedureEnv, AddColumnFamilyState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(AddColumnFamilyProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  private TableName tableName;
  private HTableDescriptor unmodifiedHTableDescriptor;
  private HColumnDescriptor cfDescriptor;
  private UserGroupInformation user;

  private List<HRegionInfo> regionInfoList;
  private Boolean traceEnabled;

  public AddColumnFamilyProcedure() {
    this.unmodifiedHTableDescriptor = null;
    this.regionInfoList = null;
    this.traceEnabled = null;
  }

  public AddColumnFamilyProcedure(
      final MasterProcedureEnv env,
      final TableName tableName,
      final HColumnDescriptor cfDescriptor) throws IOException {
    this.tableName = tableName;
    this.cfDescriptor = cfDescriptor;
    this.user = env.getRequestUser().getUGI();
    this.unmodifiedHTableDescriptor = null;
    this.regionInfoList = null;
    this.traceEnabled = null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final AddColumnFamilyState state) {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case ADD_COLUMN_FAMILY_PREPARE:
        prepareAdd(env);
        setNextState(AddColumnFamilyState.ADD_COLUMN_FAMILY_PRE_OPERATION);
        break;
      case ADD_COLUMN_FAMILY_PRE_OPERATION:
        preAdd(env, state);
        setNextState(AddColumnFamilyState.ADD_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR);
        break;
      case ADD_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR:
        updateTableDescriptor(env);
        setNextState(AddColumnFamilyState.ADD_COLUMN_FAMILY_POST_OPERATION);
        break;
      case ADD_COLUMN_FAMILY_POST_OPERATION:
        postAdd(env, state);
        setNextState(AddColumnFamilyState.ADD_COLUMN_FAMILY_REOPEN_ALL_REGIONS);
        break;
      case ADD_COLUMN_FAMILY_REOPEN_ALL_REGIONS:
        reOpenAllRegionsIfTableIsOnline(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (InterruptedException|IOException e) {
      LOG.warn("Error trying to add the column family" + getColumnFamilyName() + " to the table "
          + tableName + " (in state=" + state + ")", e);

      setFailure("master-add-columnfamily", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final AddColumnFamilyState state)
      throws IOException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }
    try {
      switch (state) {
      case ADD_COLUMN_FAMILY_REOPEN_ALL_REGIONS:
        break; // Nothing to undo.
      case ADD_COLUMN_FAMILY_POST_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo?
        break;
      case ADD_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR:
        restoreTableDescriptor(env);
        break;
      case ADD_COLUMN_FAMILY_PRE_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo?
        break;
      case ADD_COLUMN_FAMILY_PREPARE:
        break; // nothing to do
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for adding the column family"
          + getColumnFamilyName() + " to the table " + tableName, e);
      throw e;
    }
  }

  @Override
  protected AddColumnFamilyState getState(final int stateId) {
    return AddColumnFamilyState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final AddColumnFamilyState state) {
    return state.getNumber();
  }

  @Override
  protected AddColumnFamilyState getInitialState() {
    return AddColumnFamilyState.ADD_COLUMN_FAMILY_PREPARE;
  }

  @Override
  protected void setNextState(AddColumnFamilyState state) {
    if (aborted.get()) {
      setAbortFailure("add-columnfamily", "abort requested");
    } else {
      super.setNextState(state);
    }
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    aborted.set(true);
    return true;
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (!env.isInitialized()) return false;
    return env.getProcedureQueue().tryAcquireTableWrite(
      tableName,
      EventType.C_M_ADD_FAMILY.toString());
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableWrite(tableName);
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.AddColumnFamilyStateData.Builder addCFMsg =
        MasterProcedureProtos.AddColumnFamilyStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(user))
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setColumnfamilySchema(cfDescriptor.convert());
    if (unmodifiedHTableDescriptor != null) {
      addCFMsg.setUnmodifiedTableSchema(unmodifiedHTableDescriptor.convert());
    }

    addCFMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.AddColumnFamilyStateData addCFMsg =
        MasterProcedureProtos.AddColumnFamilyStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(addCFMsg.getUserInfo());
    tableName = ProtobufUtil.toTableName(addCFMsg.getTableName());
    cfDescriptor = HColumnDescriptor.convert(addCFMsg.getColumnfamilySchema());
    if (addCFMsg.hasUnmodifiedTableSchema()) {
      unmodifiedHTableDescriptor = HTableDescriptor.convert(addCFMsg.getUnmodifiedTableSchema());
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(tableName);
    sb.append(", columnfamily=");
    if (cfDescriptor != null) {
      sb.append(getColumnFamilyName());
    } else {
      sb.append("Unknown");
    }
    sb.append(") user=");
    sb.append(user);
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  /**
   * Action before any real action of adding column family.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareAdd(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table is allowed to be modified.
    MasterDDLOperationHelper.checkTableModifiable(env, tableName);

    // In order to update the descriptor, we need to retrieve the old descriptor for comparison.
    unmodifiedHTableDescriptor = env.getMasterServices().getTableDescriptors().get(tableName);
    if (unmodifiedHTableDescriptor == null) {
      throw new IOException("HTableDescriptor missing for " + tableName);
    }
    if (unmodifiedHTableDescriptor.hasFamily(cfDescriptor.getName())) {
      throw new InvalidFamilyOperationException("Column family '" + getColumnFamilyName()
          + "' in table '" + tableName + "' already exists so cannot be added");
    }
  }

  /**
   * Action before adding column family.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void preAdd(final MasterProcedureEnv env, final AddColumnFamilyState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Add the column family to the file system
   */
  private void updateTableDescriptor(final MasterProcedureEnv env) throws IOException {
    // Update table descriptor
    LOG.info("AddColumn. Table = " + tableName + " HCD = " + cfDescriptor.toString());

    HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(tableName);

    if (htd.hasFamily(cfDescriptor.getName())) {
      // It is possible to reach this situation, as we could already add the column family
      // to table descriptor, but the master failover happens before we complete this state.
      // We should be able to handle running this function multiple times without causing problem.
      return;
    }

    htd.addFamily(cfDescriptor);
    env.getMasterServices().getTableDescriptors().add(htd);
  }

  /**
   * Restore the table descriptor back to pre-add
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void restoreTableDescriptor(final MasterProcedureEnv env) throws IOException {
    HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(tableName);
    if (htd.hasFamily(cfDescriptor.getName())) {
      // Remove the column family from file system and update the table descriptor to
      // the before-add-column-family-state
      MasterDDLOperationHelper.deleteColumnFamilyFromFileSystem(env, tableName,
        getRegionInfoList(env), cfDescriptor.getName());

      env.getMasterServices().getTableDescriptors().add(unmodifiedHTableDescriptor);

      // Make sure regions are opened after table descriptor is updated.
      reOpenAllRegionsIfTableIsOnline(env);
    }
  }

  /**
   * Action after adding column family.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void postAdd(final MasterProcedureEnv env, final AddColumnFamilyState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Last action from the procedure - executed when online schema change is supported.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void reOpenAllRegionsIfTableIsOnline(final MasterProcedureEnv env) throws IOException {
    // This operation only run when the table is enabled.
    if (!env.getMasterServices().getAssignmentManager().getTableStateManager()
        .isTableState(getTableName(), TableState.State.ENABLED)) {
      return;
    }

    if (MasterDDLOperationHelper.reOpenAllRegions(env, getTableName(), getRegionInfoList(env))) {
      LOG.info("Completed add column family operation on table " + getTableName());
    } else {
      LOG.warn("Error on reopening the regions on table " + getTableName());
    }
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

  private String getColumnFamilyName() {
    return cfDescriptor.getNameAsString();
  }

  /**
   * Coprocessor Action.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final AddColumnFamilyState state)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          switch (state) {
          case ADD_COLUMN_FAMILY_PRE_OPERATION:
            cpHost.preAddColumnHandler(tableName, cfDescriptor);
            break;
          case ADD_COLUMN_FAMILY_POST_OPERATION:
            cpHost.postAddColumnHandler(tableName, cfDescriptor);
            break;
          default:
            throw new UnsupportedOperationException(this + " unhandled state=" + state);
          }
          return null;
        }
      });
    }
  }

  private List<HRegionInfo> getRegionInfoList(final MasterProcedureEnv env) throws IOException {
    if (regionInfoList == null) {
      regionInfoList = ProcedureSyncWait.getRegionsFromMeta(env, getTableName());
    }
    return regionInfoList;
  }
}

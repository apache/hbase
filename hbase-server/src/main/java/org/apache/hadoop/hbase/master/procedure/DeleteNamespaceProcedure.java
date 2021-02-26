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

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DeleteNamespaceState;

/**
 * The procedure to remove a namespace.
 */
@InterfaceAudience.Private
public class DeleteNamespaceProcedure
    extends AbstractStateMachineNamespaceProcedure<DeleteNamespaceState> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteNamespaceProcedure.class);

  private NamespaceDescriptor nsDescriptor;
  private String namespaceName;
  private Boolean traceEnabled;

  public DeleteNamespaceProcedure() {
    this.nsDescriptor = null;
    this.traceEnabled = null;
  }

  public DeleteNamespaceProcedure(final MasterProcedureEnv env, final String namespaceName) {
    this(env, namespaceName, null);
  }

  public DeleteNamespaceProcedure(final MasterProcedureEnv env, final String namespaceName,
      final ProcedurePrepareLatch latch) {
    super(env, latch);
    this.namespaceName = namespaceName;
    this.nsDescriptor = null;
    this.traceEnabled = null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final DeleteNamespaceState state)
      throws InterruptedException {
    LOG.info(this.toString());
    try {
      switch (state) {
      case DELETE_NAMESPACE_PREPARE:
        boolean present = prepareDelete(env);
        releaseSyncLatch();
        if (!present) {
          assert isFailed() : "Delete namespace should have an exception here";
          return Flow.NO_MORE_STATE;
        }
        setNextState(DeleteNamespaceState.DELETE_NAMESPACE_DELETE_FROM_NS_TABLE);
        break;
      case DELETE_NAMESPACE_DELETE_FROM_NS_TABLE:
        deleteFromNSTable(env, namespaceName);
        setNextState(DeleteNamespaceState.DELETE_NAMESPACE_REMOVE_FROM_ZK);
        break;
      case DELETE_NAMESPACE_REMOVE_FROM_ZK:
        removeFromZKNamespaceManager(env, namespaceName);
        setNextState(DeleteNamespaceState.DELETE_NAMESPACE_DELETE_DIRECTORIES);
        break;
      case DELETE_NAMESPACE_DELETE_DIRECTORIES:
        deleteDirectory(env, namespaceName);
        setNextState(DeleteNamespaceState.DELETE_NAMESPACE_REMOVE_NAMESPACE_QUOTA);
        break;
      case DELETE_NAMESPACE_REMOVE_NAMESPACE_QUOTA:
        removeNamespaceQuota(env, namespaceName);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-delete-namespace", e);
      } else {
        LOG.warn("Retriable error trying to delete namespace " + namespaceName +
          " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DeleteNamespaceState state)
      throws IOException {
    if (state == DeleteNamespaceState.DELETE_NAMESPACE_PREPARE) {
      // nothing to rollback, pre is just table-state checks.
      // We can fail if the table does not exist or is not disabled.
      // TODO: coprocessor rollback semantic is still undefined.
      releaseSyncLatch();
      return;
    }

    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final DeleteNamespaceState state) {
    switch (state) {
      case DELETE_NAMESPACE_PREPARE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DeleteNamespaceState getState(final int stateId) {
    return DeleteNamespaceState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final DeleteNamespaceState state) {
    return state.getNumber();
  }

  @Override
  protected DeleteNamespaceState getInitialState() {
    return DeleteNamespaceState.DELETE_NAMESPACE_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.DeleteNamespaceStateData.Builder deleteNamespaceMsg =
        MasterProcedureProtos.DeleteNamespaceStateData.newBuilder().setNamespaceName(namespaceName);
    if (this.nsDescriptor != null) {
      deleteNamespaceMsg.setNamespaceDescriptor(
        ProtobufUtil.toProtoNamespaceDescriptor(this.nsDescriptor));
    }
    serializer.serialize(deleteNamespaceMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.DeleteNamespaceStateData deleteNamespaceMsg =
        serializer.deserialize(MasterProcedureProtos.DeleteNamespaceStateData.class);
    namespaceName = deleteNamespaceMsg.getNamespaceName();
    if (deleteNamespaceMsg.hasNamespaceDescriptor()) {
      nsDescriptor =
          ProtobufUtil.toNamespaceDescriptor(deleteNamespaceMsg.getNamespaceDescriptor());
    }
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  @Override
  protected String getNamespaceName() {
    return namespaceName;
  }

  /**
   * Action before any real action of deleting namespace.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private boolean prepareDelete(final MasterProcedureEnv env) throws IOException {
    if (getTableNamespaceManager(env).doesNamespaceExist(namespaceName) == false) {
      setFailure("master-delete-namespace", new NamespaceNotFoundException(namespaceName));
      return false;
    }
    if (NamespaceDescriptor.RESERVED_NAMESPACES.contains(namespaceName)) {
      setFailure("master-delete-namespace", new ConstraintException(
          "Reserved namespace "+ namespaceName +" cannot be removed."));
      return false;
    }

    int tableCount = 0;
    try {
      tableCount = env.getMasterServices().listTableDescriptorsByNamespace(namespaceName).size();
    } catch (FileNotFoundException fnfe) {
      setFailure("master-delete-namespace", new NamespaceNotFoundException(namespaceName));
      return false;
    }
    if (tableCount > 0) {
      setFailure("master-delete-namespace", new ConstraintException(
          "Only empty namespaces can be removed. Namespace "+ namespaceName + " has "
          + tableCount +" tables"));
      return false;
    }

    // This is used for rollback
    nsDescriptor = getTableNamespaceManager(env).get(namespaceName);
    return true;
  }

  /**
   * delete the row from namespace table
   * @param env MasterProcedureEnv
   * @param namespaceName name of the namespace in string format
   * @throws IOException
   */
  protected static void deleteFromNSTable(
      final MasterProcedureEnv env,
      final String namespaceName) throws IOException {
    getTableNamespaceManager(env).removeFromNSTable(namespaceName);
  }

  /**
   * undo the delete
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void undoDeleteFromNSTable(final MasterProcedureEnv env) {
    try {
      if (nsDescriptor != null) {
        CreateNamespaceProcedure.insertIntoNSTable(env, nsDescriptor);
      }
    } catch (Exception e) {
      // Ignore
      LOG.debug("Rollback of deleteFromNSTable throws exception: " + e);
    }
  }

  /**
   * remove from ZooKeeper.
   * @param env MasterProcedureEnv
   * @param namespaceName name of the namespace in string format
   * @throws IOException
   */
  protected static void removeFromZKNamespaceManager(
      final MasterProcedureEnv env,
      final String namespaceName) throws IOException {
    getTableNamespaceManager(env).removeFromZKNamespaceManager(namespaceName);
  }

  /**
   * undo the remove from ZooKeeper
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void undoRemoveFromZKNamespaceManager(final MasterProcedureEnv env) {
    try {
      if (nsDescriptor != null) {
        CreateNamespaceProcedure.updateZKNamespaceManager(env, nsDescriptor);
      }
    } catch (Exception e) {
      // Ignore
      LOG.debug("Rollback of removeFromZKNamespaceManager throws exception: " + e);
    }
  }

  /**
   * Delete the namespace directories from the file system
   * @param env MasterProcedureEnv
   * @param namespaceName name of the namespace in string format
   * @throws IOException
   */
  protected static void deleteDirectory(
      final MasterProcedureEnv env,
      final String namespaceName) throws IOException {
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    FileSystem fs = mfs.getFileSystem();
    Path p = CommonFSUtils.getNamespaceDir(mfs.getRootDir(), namespaceName);

    try {
      for(FileStatus status : fs.listStatus(p)) {
        if (!HConstants.HBASE_NON_TABLE_DIRS.contains(status.getPath().getName())) {
          throw new IOException("Namespace directory contains table dir: " + status.getPath());
        }
      }
      if (!fs.delete(CommonFSUtils.getNamespaceDir(mfs.getRootDir(), namespaceName), true)) {
        throw new IOException("Failed to remove namespace: " + namespaceName);
      }
    } catch (FileNotFoundException e) {
      // File already deleted, continue
      LOG.debug("deleteDirectory throws exception: " + e);
    }
  }

  /**
   * undo delete directory
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void rollbackDeleteDirectory(final MasterProcedureEnv env) throws IOException {
    try {
      CreateNamespaceProcedure.createDirectory(env, nsDescriptor);
    } catch (Exception e) {
      // Ignore exception
      LOG.debug("Rollback of deleteDirectory throws exception: " + e);
    }
  }

  /**
   * remove quota for the namespace
   * @param env MasterProcedureEnv
   * @param namespaceName name of the namespace in string format
   * @throws IOException
   **/
  protected static void removeNamespaceQuota(
      final MasterProcedureEnv env,
      final String namespaceName) throws IOException {
    env.getMasterServices().getMasterQuotaManager().removeNamespaceQuota(namespaceName);
  }

  /**
   * undo remove quota for the namespace
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void rollbacRemoveNamespaceQuota(final MasterProcedureEnv env) throws IOException {
    try {
      CreateNamespaceProcedure.setNamespaceQuota(env, nsDescriptor);
    } catch (Exception e) {
      // Ignore exception
      LOG.debug("Rollback of removeNamespaceQuota throws exception: " + e);
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
}

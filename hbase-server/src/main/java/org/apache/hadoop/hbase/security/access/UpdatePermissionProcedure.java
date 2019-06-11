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
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.AclProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.UpdatePermissionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@InterfaceAudience.Private
public class UpdatePermissionProcedure
    extends StateMachineProcedure<MasterProcedureEnv, UpdatePermissionState>
    implements AclProcedureInterface {
  private static Logger LOG = LoggerFactory.getLogger(UpdatePermissionProcedure.class);
  // the entry represents that need to reload all kinds of permission cache
  // because namespace and table can not contain a '#' character
  static final byte[] RELOAD_ALL_ENTRY = Bytes.toBytes("#Reload");

  public enum UpdatePermissionType {
    GRANT, REVOKE, DELETE_TABLE, DELETE_NAMESPACE, RELOAD
  }

  private UpdatePermissionType updatePermissionType;
  private ProcedurePrepareLatch procedurePrepareLatch;
  private ServerName serverName;
  private RetryCounter retryCounter;

  private byte[] entry;
  private UserPermission userPermission;
  private boolean mergeExistingPermissions;

  public UpdatePermissionProcedure() {
  }

  public UpdatePermissionProcedure(UpdatePermissionType type, ServerName serverName,
      Optional<UserPermission> userPermission, Optional<Boolean> mergeExistingPermissions,
      Optional<String> deleteEntry, ProcedurePrepareLatch latch) {
    this.updatePermissionType = type;
    this.serverName = serverName;
    this.procedurePrepareLatch = latch;
    if (updatePermissionType == UpdatePermissionType.GRANT) {
      if (!userPermission.isPresent()) {
        throw new IllegalArgumentException("UserPermission is empty");
      }
      if (!mergeExistingPermissions.isPresent()) {
        throw new IllegalArgumentException("mergeExistingPermissions is empty");
      }
      this.userPermission = userPermission.get();
      this.mergeExistingPermissions = mergeExistingPermissions.get();
      this.entry = PermissionStorage.userPermissionRowKey(this.userPermission.getPermission());
    } else if (updatePermissionType == UpdatePermissionType.REVOKE) {
      if (!userPermission.isPresent()) {
        throw new IllegalArgumentException("UserPermission is empty");
      }
      this.userPermission = userPermission.get();
      this.entry = PermissionStorage.userPermissionRowKey(this.userPermission.getPermission());
    } else if (updatePermissionType == UpdatePermissionType.DELETE_NAMESPACE) {
      if (!deleteEntry.isPresent()) {
        throw new IllegalArgumentException("Namespace is empty");
      }
      this.entry = Bytes.toBytes(PermissionStorage.toNamespaceEntry(deleteEntry.get()));
    } else if (updatePermissionType == UpdatePermissionType.DELETE_TABLE) {
      if (!deleteEntry.isPresent()) {
        throw new IllegalArgumentException("Table is empty");
      }
      this.entry = Bytes.toBytes(deleteEntry.get());
    } else if (updatePermissionType == UpdatePermissionType.RELOAD) {
      this.entry = RELOAD_ALL_ENTRY;
      LOG.info("Reload all permission cache");
    } else {
      throw new IllegalArgumentException("Unknown update permission type");
    }
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, UpdatePermissionState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case UPDATE_PERMISSION_STORAGE:
        try {
          // update permission in acl table and znode, refresh master auth manager
          updateStorageAndRefreshAuthManager(env);
        } catch (IOException e) {
          if (retryCounter == null) {
            retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
          }
          long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
          LOG.warn("Failed to update user permission, type {}, entry {}, sleep {} secs and retry",
            updatePermissionType, Bytes.toString(entry), backoff / 1000, e);
          setTimeout(Math.toIntExact(backoff));
          setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
          skipPersistence();
          throw new ProcedureSuspendedException();
        }
        setNextState(UpdatePermissionState.UPDATE_PERMISSION_CACHE_ON_RS);
        return Flow.HAS_MORE_STATE;
      case UPDATE_PERMISSION_CACHE_ON_RS:
        // update permission in RS auth manager cache
        UpdatePermissionRemoteProcedure[] subProcedures =
            env.getMasterServices().getServerManager().getOnlineServersList().stream()
                .map(sn -> new UpdatePermissionRemoteProcedure(sn, Bytes.toString(entry)))
                .toArray(UpdatePermissionRemoteProcedure[]::new);
        addChildProcedure(subProcedures);
        setNextState(UpdatePermissionState.POST_UPDATE_PERMISSION);
        return Flow.HAS_MORE_STATE;
      case POST_UPDATE_PERMISSION:
        try {
          postUpdatePermission(env);
        } catch (IOException e) {
          LOG.warn(
            "{} failed to call post CP hook after updating permission, type {}, userPermission {} "
                + "ignore since the procedure has already done",
            getClass().getName(), updatePermissionType, e);
        }
        releaseProcedureLatch();
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  private void postUpdatePermission(MasterProcedureEnv env) throws IOException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      if (updatePermissionType == UpdatePermissionType.GRANT) {
        cpHost.postGrant(userPermission, mergeExistingPermissions);
      } else if (updatePermissionType == UpdatePermissionType.REVOKE) {
        cpHost.postRevoke(userPermission);
      }
    }
  }

  private void releaseProcedureLatch() {
    ProcedurePrepareLatch.releaseLatch(procedurePrepareLatch, this);
  }

  private void updateStorageAndRefreshAuthManager(MasterProcedureEnv env) throws IOException {
    try (Table table =
        env.getMasterServices().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
      Configuration conf = env.getMasterConfiguration();
      AuthManager authManager = env.getMasterServices().getAccessChecker().getAuthManager();
      ZKPermissionStorage zkPermissionStorage = env.getMasterServices().getZKPermissionStorage();
      if (updatePermissionType == UpdatePermissionType.GRANT) {
        // add user permission to acl table
        PermissionStorage.addUserPermission(conf, userPermission, table, mergeExistingPermissions);
        // get permissions from acl table and write to zk
        ListMultimap<String, UserPermission> permissions =
            PermissionStorage.getPermissions(conf, entry, table, null, null, null, false);
        byte[] userPermissions = PermissionStorage.writePermissionsAsBytes(permissions);
        zkPermissionStorage.writePermission(entry, userPermissions);
        // update permission in master auth manager cache
        authManager.refresh(Bytes.toString(entry), userPermissions);
      } else if (updatePermissionType == UpdatePermissionType.REVOKE) {
        // remove user permission from acl table
        PermissionStorage.removeUserPermission(conf, userPermission, table);
        // get permissions from acl table and write to zk
        ListMultimap<String, UserPermission> permissions =
                PermissionStorage.getPermissions(conf, entry, table, null, null, null, false);
        byte[] userPermissions = PermissionStorage.writePermissionsAsBytes(permissions);
        zkPermissionStorage.writePermission(entry, userPermissions);
        // update permission in master auth manager cache
        authManager.refresh(Bytes.toString(entry), userPermissions);
      } else if (updatePermissionType == UpdatePermissionType.DELETE_NAMESPACE) {
        String namespace = Bytes.toString(PermissionStorage.fromNamespaceEntry(entry));
        // remove all namespace permissions from acl table
        PermissionStorage.removeNamespacePermissions(env.getMasterConfiguration(), namespace,
                table);
        // remove namespace acl znode from zk
        zkPermissionStorage.deleteNamespacePermission(namespace);
        // remove all namespace permission from master auth manager cache
        env.getMasterServices().getAccessChecker().getAuthManager().remove(Bytes.toString(entry));
      } else if (updatePermissionType == UpdatePermissionType.DELETE_TABLE) {
        TableName tableName = TableName.valueOf(entry);
        // remove all table permissions from acl table
        PermissionStorage.removeTablePermissions(env.getMasterConfiguration(), tableName, table);
        // remove table acl znode from zk
        zkPermissionStorage.deleteTablePermission(tableName);
        // remove all table permission from master auth manager cache
        env.getMasterServices().getAccessChecker().getAuthManager().remove(Bytes.toString(entry));
      } else if (updatePermissionType == UpdatePermissionType.RELOAD) {
        // load all permissions from acl table
        Map<byte[], ListMultimap<String, UserPermission>> permissions =
            PermissionStorage.loadAll(table);
        // write all permissions to zk
        zkPermissionStorage.reloadPermissions(permissions);
        // reload master auth manager permission cache
        zkPermissionStorage.reloadPermissionsToAuthManager(
          env.getMasterServices().getAccessChecker().getAuthManager());
      }
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, UpdatePermissionState state)
      throws IOException, InterruptedException {
    if (state == getInitialState()) {
      return;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  protected UpdatePermissionState getState(int stateId) {
    return UpdatePermissionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(UpdatePermissionState accessControlState) {
    return accessControlState.getNumber();
  }

  @Override
  protected UpdatePermissionState getInitialState() {
    return UpdatePermissionState.UPDATE_PERMISSION_STORAGE;
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" server=").append(serverName);
    sb.append(", type=").append(updatePermissionType);
    if (updatePermissionType == UpdatePermissionType.GRANT) {
      sb.append(", user permission=").append(userPermission);
      sb.append(", mergeExistingPermissions=").append(mergeExistingPermissions);
    } else if (updatePermissionType == UpdatePermissionType.REVOKE) {
      sb.append(", user permission=").append(userPermission);
    } else if (updatePermissionType == UpdatePermissionType.DELETE_NAMESPACE
        || updatePermissionType == UpdatePermissionType.DELETE_TABLE) {
      sb.append(", delete permission entry=").append(Bytes.toString(entry));
    }
  }

  @Override
  public String getAclEntry() {
    return Bytes.toString(entry);
  }

  @Override
  public AclOperationType getAclOperationType() {
    return AclOperationType.UPDATE;
  }
}

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
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class MasterProcedureUtil {

  private MasterProcedureUtil() {}

  public static UserInformation toProtoUserInfo(User user) {
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    userInfoPB.setEffectiveUser(user.getName());
    if (user.getUGI().getRealUser() != null) {
      userInfoPB.setRealUser(user.getUGI().getRealUser().getUserName());
    }
    return userInfoPB.build();
  }

  public static User toUserInfo(UserInformation userInfoProto) {
    if (userInfoProto.hasEffectiveUser()) {
      String effectiveUser = userInfoProto.getEffectiveUser();
      if (userInfoProto.hasRealUser()) {
        String realUser = userInfoProto.getRealUser();
        UserGroupInformation realUserUgi = UserGroupInformation.createRemoteUser(realUser);
        return User.create(UserGroupInformation.createProxyUser(effectiveUser, realUserUgi));
      }
      return User.create(UserGroupInformation.createRemoteUser(effectiveUser));
    }
    return null;
  }

  /**
   * Helper Runnable used in conjunction with submitProcedure() to deal with
   * submitting procs with nonce.
   * See submitProcedure() for an example.
   */
  public static abstract class NonceProcedureRunnable {
    private final MasterServices master;
    private final NonceKey nonceKey;
    private Long procId;

    public NonceProcedureRunnable(final MasterServices master,
        final long nonceGroup, final long nonce) {
      this.master = master;
      this.nonceKey = getProcedureExecutor().createNonceKey(nonceGroup, nonce);
    }

    protected NonceKey getNonceKey() {
      return nonceKey;
    }

    protected MasterServices getMaster() {
      return master;
    }

    protected ProcedureExecutor<MasterProcedureEnv> getProcedureExecutor() {
      return master.getMasterProcedureExecutor();
    }

    protected long getProcId() {
      return procId != null ? procId.longValue() : -1;
    }

    protected long setProcId(final long procId) {
      this.procId = procId;
      return procId;
    }

    protected abstract void run() throws IOException;
    protected abstract String getDescription();

    protected long submitProcedure(final Procedure<MasterProcedureEnv> proc) {
      assert procId == null : "submitProcedure() was already called, running procId=" + procId;
      procId = getProcedureExecutor().submitProcedure(proc, nonceKey);
      return procId;
    }
  }

  /**
   * Helper used to deal with submitting procs with nonce.
   * Internally the NonceProcedureRunnable.run() will be called only if no one else
   * registered the nonce. any Exception thrown by the run() method will be
   * collected/handled and rethrown.
   * <code>
   * long procId = MasterProcedureUtil.submitProcedure(
   *      new NonceProcedureRunnable(procExec, nonceGroup, nonce) {
   *   {@literal @}Override
   *   public void run() {
   *     cpHost.preOperation();
   *     submitProcedure(new MyProc());
   *     cpHost.postOperation();
   *   }
   * });
   * </code>
   */
  public static long submitProcedure(final NonceProcedureRunnable runnable) throws IOException {
    final ProcedureExecutor<MasterProcedureEnv> procExec = runnable.getProcedureExecutor();
    final long procId = procExec.registerNonce(runnable.getNonceKey());
    if (procId >= 0) return procId; // someone already registered the nonce
    try {
      runnable.run();
    } catch (IOException e) {
      procExec.setFailureResultForNonce(runnable.getNonceKey(),
          runnable.getDescription(),
          procExec.getEnvironment().getRequestUser(), e);
      throw e;
    } finally {
      procExec.unregisterNonceIfProcedureWasNotSubmitted(runnable.getNonceKey());
    }
    return runnable.getProcId();
  }

  /**
   * Pattern used to validate a Procedure WAL file name see
   * {@link #validateProcedureWALFilename(String)} for description.
   * @deprecated Since 2.3.0, will be removed in 4.0.0. We do not use this style of procedure wal
   *             file name any more.
   */
  @Deprecated
  private static final Pattern PATTERN = Pattern.compile(".*pv2-\\d{20}.log");

  /**
   * A Procedure WAL file name is of the format: pv-&lt;wal-id&gt;.log where wal-id is 20 digits.
   * @param filename name of the file to validate
   * @return <tt>true</tt> if the filename matches a Procedure WAL, <tt>false</tt> otherwise
   */
  public static boolean validateProcedureWALFilename(String filename) {
    return PATTERN.matcher(filename).matches();
  }

  /**
   * Return the priority for the given table. Now meta table is 3, other system tables are 2, and
   * user tables are 1.
   */
  public static int getTablePriority(TableName tableName) {
    if (TableName.isMetaTableName(tableName)) {
      return 3;
    } else if (tableName.isSystemTable()) {
      return 2;
    } else {
      return 1;
    }
  }

  /**
   * Return the priority for the given procedure. For now we only have two priorities, 100 for
   * server carrying meta, and 1 for others.
   */
  public static int getServerPriority(ServerProcedureInterface proc) {
    return proc.hasMetaTableRegion() ? 100 : 1;
  }

  /**
   * This is a version of unwrapRemoteIOException that can do DoNotRetryIOE.
   * We need to throw DNRIOE to clients if a failed Procedure else they will
   * keep trying. The default proc.getException().unwrapRemoteException
   * doesn't have access to DNRIOE from the procedure2 module.
   */
  public static IOException unwrapRemoteIOException(Procedure<?> proc) {
    Exception e = proc.getException().unwrapRemoteException();
    // Do not retry ProcedureExceptions!
    return (e instanceof ProcedureException)? new DoNotRetryIOException(e):
        proc.getException().unwrapRemoteIOException();
  }

  /**
   * Do not allow creating new tables/namespaces which has an empty rs group, expect the default rs
   * group. Notice that we do not check for online servers, as this is not stable because region
   * servers can die at any time.
   */
  public static void checkGroupNotEmpty(RSGroupInfo rsGroupInfo, Supplier<String> forWhom)
    throws ConstraintException {
    if (rsGroupInfo == null || rsGroupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
      // we do not have a rs group config or we explicitly set the rs group to default, then no need
      // to check.
      return;
    }
    if (rsGroupInfo.getServers().isEmpty()) {
      throw new ConstraintException(
        "No servers in the rsgroup " + rsGroupInfo.getName() + " for " + forWhom.get());
    }
  }

  @FunctionalInterface
  public interface RSGroupGetter {
    RSGroupInfo get(String groupName) throws IOException;
  }

  public static RSGroupInfo checkGroupExists(RSGroupGetter getter, Optional<String> optGroupName,
    Supplier<String> forWhom) throws IOException {
    if (optGroupName.isPresent()) {
      String groupName = optGroupName.get();
      RSGroupInfo group = getter.get(groupName);
      if (group == null) {
        throw new ConstraintException(
          "Region server group " + groupName + " for " + forWhom.get() + " does not exit");
      }
      return group;
    }
    return null;
  }

  public static Optional<String> getNamespaceGroup(NamespaceDescriptor namespaceDesc) {
    return Optional
      .ofNullable(namespaceDesc.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP));
  }

  public static boolean waitInitialized(Procedure<MasterProcedureEnv> proc, MasterProcedureEnv env,
    TableName tableName) {
    if (TableName.isMetaTableName(tableName)) {
      return false;
    }
    // we need meta to be loaded
    AssignmentManager am = env.getAssignmentManager();
    return am.waitMetaLoaded(proc);
  }
}

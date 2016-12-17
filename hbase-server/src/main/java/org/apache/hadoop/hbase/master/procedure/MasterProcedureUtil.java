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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class MasterProcedureUtil {
  private static final Log LOG = LogFactory.getLog(MasterProcedureUtil.class);

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

    protected long submitProcedure(final Procedure proc) {
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
}

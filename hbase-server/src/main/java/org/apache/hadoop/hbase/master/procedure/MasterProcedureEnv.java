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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterProcedureEnv implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcedureEnv.class);

  @InterfaceAudience.Private
  public static class FsUtilsLeaseRecovery implements LeaseRecovery {
    private final MasterServices master;

    public FsUtilsLeaseRecovery(final MasterServices master) {
      this.master = master;
    }

    @Override
    public void recoverFileLease(final FileSystem fs, final Path path) throws IOException {
      final Configuration conf = master.getConfiguration();
      RecoverLeaseFSUtils.recoverFileLease(fs, path, conf, new CancelableProgressable() {
        @Override
        public boolean progress() {
          LOG.debug("Recover Procedure Store log lease: " + path);
          return isRunning();
        }
      });
    }

    private boolean isRunning() {
      return !master.isStopped() && !master.isStopping() && !master.isAborted();
    }
  }

  private final RSProcedureDispatcher remoteDispatcher;
  private final MasterProcedureScheduler procSched;
  private final MasterServices master;

  public MasterProcedureEnv(final MasterServices master) {
    this(master, new RSProcedureDispatcher(master));
  }

  public MasterProcedureEnv(final MasterServices master,
      final RSProcedureDispatcher remoteDispatcher) {
    this.master = master;
    this.procSched = new MasterProcedureScheduler(
      procId -> master.getMasterProcedureExecutor().getProcedure(procId));
    this.remoteDispatcher = remoteDispatcher;
  }

  public User getRequestUser() {
    return RpcServer.getRequestUser().orElse(Superusers.getSystemUser());
  }

  public MasterServices getMasterServices() {
    return master;
  }

  public Configuration getMasterConfiguration() {
    return master.getConfiguration();
  }

  public AssignmentManager getAssignmentManager() {
    return master.getAssignmentManager();
  }

  public MasterCoprocessorHost getMasterCoprocessorHost() {
    return master.getMasterCoprocessorHost();
  }

  public MasterProcedureScheduler getProcedureScheduler() {
    return procSched;
  }

  public RSProcedureDispatcher getRemoteDispatcher() {
    return remoteDispatcher;
  }

  public ReplicationPeerManager getReplicationPeerManager() {
    return master.getReplicationPeerManager();
  }

  public MasterFileSystem getMasterFileSystem() {
    return master.getMasterFileSystem();
  }

  public boolean isRunning() {
    if (this.master == null || this.master.getMasterProcedureExecutor() == null) return false;
    return master.getMasterProcedureExecutor().isRunning();
  }

  public boolean isInitialized() {
    return master.isInitialized();
  }

  public boolean waitInitialized(Procedure<?> proc) {
    return master.getInitializedEvent().suspendIfNotReady(proc);
  }

  public void setEventReady(ProcedureEvent<?> event, boolean isReady) {
    if (isReady) {
      event.wake(procSched);
    } else {
      event.suspend();
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    master.getMasterProcedureExecutor().refreshConfiguration(conf);
  }
}

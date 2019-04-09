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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionRemoteProcedureBaseState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionRemoteProcedureBaseStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The base class for the remote procedures used to open/close a region.
 * <p/>
 * Notice that here we do not care about the result of the remote call, if the remote call is
 * finished, either succeeded or not, we will always finish the procedure. The parent procedure
 * should take care of the result and try to reschedule if the result is not good.
 */
@InterfaceAudience.Private
public abstract class RegionRemoteProcedureBase extends Procedure<MasterProcedureEnv>
    implements TableProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionRemoteProcedureBase.class);

  protected RegionInfo region;

  protected ServerName targetServer;

  private RegionRemoteProcedureBaseState state =
    RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH;

  private TransitionCode transitionCode;

  private long seqId;

  private int attempt;

  protected RegionRemoteProcedureBase() {
  }

  protected RegionRemoteProcedureBase(TransitRegionStateProcedure parent, RegionInfo region,
      ServerName targetServer) {
    this.region = region;
    this.targetServer = targetServer;
    parent.attachRemoteProc(this);
  }

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    // should not be called since we use reportRegionStateTransition to report the result
    throw new UnsupportedOperationException();
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    // should not be called since we use reportRegionStateTransition to report the result
    throw new UnsupportedOperationException();
  }

  private RegionStateNode getRegionNode(MasterProcedureEnv env) {
    return env.getAssignmentManager().getRegionStates().getRegionStateNode(region);
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName remote, IOException exception) {
    RegionStateNode regionNode = getRegionNode(env);
    regionNode.lock();
    try {
      if (!env.getMasterServices().getServerManager().isServerOnline(remote)) {
        // the SCP will interrupt us, give up
        LOG.debug("{} for region {}, targetServer {} is dead, SCP will interrupt us, give up", this,
          regionNode, remote);
        return;
      }
      if (state != RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH) {
        // not sure how can this happen but anyway let's add a check here to avoid waking the wrong
        // procedure...
        LOG.warn("{} for region {}, targetServer={} has already been woken up, ignore", this,
          regionNode, remote);
        return;
      }
      LOG.warn("The remote operation {} for region {} to server {} failed", this, regionNode,
        remote, exception);
      // It is OK to not persist the state here, as we do not need to change the region state if the
      // remote call is failed. If the master crashed before we actually execute the procedure and
      // persist the new state, it is fine to retry on the same target server again.
      state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH_FAIL;
      regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  public TableName getTableName() {
    return region.getTable();
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    if (TableName.isMetaTableName(getTableName())) {
      return false;
    }
    // First we need meta to be loaded, and second, if meta is not online then we will likely to
    // fail when updating meta so we wait until it is assigned.
    AssignmentManager am = env.getAssignmentManager();
    return am.waitMetaLoaded(this) || am.waitMetaAssigned(this, region);
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  // do some checks to see if the report is valid, without actually updating meta.
  protected abstract void reportTransition(RegionStateNode regionNode,
      TransitionCode transitionCode, long seqId) throws IOException;

  // A bit strange but the procedure store will throw RuntimeException if we can not persist the
  // state, so upper layer should take care of this...
  private void persistAndWake(MasterProcedureEnv env, RegionStateNode regionNode) {
    env.getMasterServices().getMasterProcedureExecutor().getStore().update(this);
    regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
  }

  // should be called with RegionStateNode locked, to avoid race with the execute method below
  void reportTransition(MasterProcedureEnv env, RegionStateNode regionNode, ServerName serverName,
      TransitionCode transitionCode, long seqId) throws IOException {
    if (state != RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH) {
      // should be a retry
      return;
    }
    if (!targetServer.equals(serverName)) {
      throw new UnexpectedStateException("Received report from " + serverName + ", expected " +
        targetServer + ", " + regionNode + ", proc=" + this);
    }
    reportTransition(regionNode, transitionCode, seqId);
    // this state means we have received the report from RS, does not mean the result is fine, as we
    // may received a FAILED_OPEN.
    this.state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_REPORT_SUCCEED;
    this.transitionCode = transitionCode;
    this.seqId = seqId;
    // Persist the transition code and openSeqNum(if provided).
    // We should not update the hbase:meta directly as this may cause races when master restarts,
    // as the old active master may incorrectly report back to RS and cause the new master to hang
    // on a OpenRegionProcedure forever. See HBASE-22060 and HBASE-22074 for more details.
    boolean succ = false;
    try {
      persistAndWake(env, regionNode);
      succ = true;
    } finally {
      if (!succ) {
        this.state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH;
        this.transitionCode = null;
        this.seqId = HConstants.NO_SEQNUM;
      }
    }
  }

  void serverCrashed(MasterProcedureEnv env, RegionStateNode regionNode, ServerName serverName) {
    if (state != RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH) {
      // should be a retry
      return;
    }
    this.state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_SERVER_CRASH;
    boolean succ = false;
    try {
      persistAndWake(env, regionNode);
      succ = true;
    } finally {
      if (!succ) {
        this.state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH;
      }
    }
  }

  private TransitRegionStateProcedure getParent(MasterProcedureEnv env) {
    return (TransitRegionStateProcedure) env.getMasterServices().getMasterProcedureExecutor()
      .getProcedure(getParentProcId());
  }

  private void unattach(MasterProcedureEnv env) {
    getParent(env).unattachRemoteProc(this);
  }

  // actually update the state to meta
  protected abstract void updateTransition(MasterProcedureEnv env, RegionStateNode regionNode,
      TransitionCode transitionCode, long seqId) throws IOException;

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    RegionStateNode regionNode = getRegionNode(env);
    regionNode.lock();
    try {
      switch (state) {
        case REGION_REMOTE_PROCEDURE_DISPATCH: {
          // The code which wakes us up also needs to lock the RSN so here we do not need to
          // synchronize
          // on the event.
          ProcedureEvent<?> event = regionNode.getProcedureEvent();
          try {
            env.getRemoteDispatcher().addOperationToNode(targetServer, this);
          } catch (FailedRemoteDispatchException e) {
            LOG.warn("Can not add remote operation {} for region {} to server {}, this usually " +
              "because the server is alread dead, give up and mark the procedure as complete, " +
              "the parent procedure will take care of this.", this, region, targetServer, e);
            unattach(env);
            return null;
          }
          event.suspend();
          event.suspendIfNotReady(this);
          throw new ProcedureSuspendedException();
        }
        case REGION_REMOTE_PROCEDURE_REPORT_SUCCEED:
          updateTransition(env, regionNode, transitionCode, seqId);
          unattach(env);
          return null;
        case REGION_REMOTE_PROCEDURE_DISPATCH_FAIL:
          // the remote call is failed so we do not need to change the region state, just return.
          unattach(env);
          return null;
        case REGION_REMOTE_PROCEDURE_SERVER_CRASH:
          env.getAssignmentManager().regionClosed(regionNode, false);
          unattach(env);
          return null;
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    } catch (IOException e) {
      long backoff = ProcedureUtil.getBackoffTimeMs(this.attempt++);
      LOG.warn("Failed updating meta, suspend {}secs {}; {};", backoff / 1000, this, regionNode, e);
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  public boolean storeInDispatchedQueue() {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RegionRemoteProcedureBaseStateData.Builder builder =
      RegionRemoteProcedureBaseStateData.newBuilder().setRegion(ProtobufUtil.toRegionInfo(region))
        .setTargetServer(ProtobufUtil.toServerName(targetServer)).setState(state);
    if (transitionCode != null) {
      builder.setTransitionCode(transitionCode);
      builder.setSeqId(seqId);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RegionRemoteProcedureBaseStateData data =
      serializer.deserialize(RegionRemoteProcedureBaseStateData.class);
    region = ProtobufUtil.toRegionInfo(data.getRegion());
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    state = data.getState();
    if (data.hasTransitionCode()) {
      transitionCode = data.getTransitionCode();
      seqId = data.getSeqId();
    }
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    getParent(env).attachRemoteProc(this);
  }
}

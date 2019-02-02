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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionRemoteProcedureBaseStateData;

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

  private ServerName targetServer;

  private boolean dispatched;

  protected RegionRemoteProcedureBase() {
  }

  protected RegionRemoteProcedureBase(RegionInfo region, ServerName targetServer) {
    this.region = region;
    this.targetServer = targetServer;
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
      LOG.warn("The remote operation {} for region {} to server {} failed", this, region,
        targetServer, exception);
      // This could happen as the RSProcedureDispatcher and dead server processor are executed in
      // different threads. It is possible that we have already scheduled SCP for the targetServer
      // and woken up this procedure, and assigned the region to another RS, and then the
      // RSProcedureDispatcher notices that the targetServer is dead so it can not send the request
      // out and call remoteCallFailed, which makes us arrive here, especially that if the target
      // machine is completely down, which means you can only receive a ConnectionTimeout after a
      // very long time(depends on the timeout settings and in HBase usually it will be at least 15
      // seconds, or even 1 minute). So here we need to check whether we are stilling waiting on the
      // given event, if not, this means that we have already been woken up so do not wake it up
      // again.
      if (!regionNode.getProcedureEvent().wakeIfSuspended(env.getProcedureScheduler(), this)) {
        LOG.warn("{} is not waiting on the event for region {}, targer server = {}, ignore.", this,
          region, targetServer);
      }
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

  /**
   * Check whether we still need to make the call to RS.
   * <p/>
   * This could happen when master restarts. Since we do not know whether a request has already been
   * sent to the region server after we add a remote operation to the dispatcher, so the safe way is
   * to not persist the dispatched field and try to add the remote operation again. But it is
   * possible that we do have already sent the request to region server and it has also sent back
   * the response, so here we need to check the region state, if it is not in the expecting state,
   * we should give up, otherwise we may hang for ever, as the region server will just ignore
   * redundant calls.
   */
  protected abstract boolean shouldDispatch(RegionStateNode regionNode);

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      // we are done, the parent procedure will check whether we are succeeded.
      return null;
    }
    RegionStateNode regionNode = getRegionNode(env);
    regionNode.lock();
    try {
      if (!shouldDispatch(regionNode)) {
        return null;
      }
      // The code which wakes us up also needs to lock the RSN so here we do not need to synchronize
      // on the event.
      ProcedureEvent<?> event = regionNode.getProcedureEvent();
      try {
        env.getRemoteDispatcher().addOperationToNode(targetServer, this);
      } catch (FailedRemoteDispatchException e) {
        LOG.warn("Can not add remote operation {} for region {} to server {}, this usually " +
          "because the server is alread dead, give up and mark the procedure as complete, " +
          "the parent procedure will take care of this.", this, region, targetServer, e);
        return null;
      }
      dispatched = true;
      event.suspend();
      event.suspendIfNotReady(this);
      throw new ProcedureSuspendedException();
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(
      RegionRemoteProcedureBaseStateData.newBuilder().setRegion(ProtobufUtil.toRegionInfo(region))
        .setTargetServer(ProtobufUtil.toServerName(targetServer)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RegionRemoteProcedureBaseStateData data =
      serializer.deserialize(RegionRemoteProcedureBaseStateData.class);
    region = ProtobufUtil.toRegionInfo(data.getRegion());
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
  }
}

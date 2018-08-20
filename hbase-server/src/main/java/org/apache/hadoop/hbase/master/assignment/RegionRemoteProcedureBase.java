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

  private ProcedureEvent<?> getRegionEvent(MasterProcedureEnv env) {
    return env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(region)
      .getProcedureEvent();
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName remote,
      IOException exception) {
    ProcedureEvent<?> event = getRegionEvent(env);
    synchronized (event) {
      if (event.isReady()) {
        LOG.warn(
          "The procedure event of procedure {} for region {} to server {} is not suspended, " +
            "usually this should not happen, but anyway let's skip the following wake up code, ",
          this, region, targetServer);
        return;
      }
      LOG.warn("The remote operation {} for region {} to server {} failed", this, region,
        targetServer, exception);
      event.wake(env.getProcedureScheduler());
    }
  }

  @Override
  public TableName getTableName() {
    return region.getTable();
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      // we are done, the parent procedure will check whether we are succeeded.
      return null;
    }
    ProcedureEvent<?> event = getRegionEvent(env);
    synchronized (event) {
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
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(RegionRemoteProcedureBaseStateData.newBuilder()
      .setRegion(ProtobufUtil.toRegionInfo(region))
      .setTargetServer(ProtobufUtil.toServerName(targetServer)).setDispatched(dispatched).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RegionRemoteProcedureBaseStateData data =
      serializer.deserialize(RegionRemoteProcedureBaseStateData.class);
    region = ProtobufUtil.toRegionInfo(data.getRegion());
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    dispatched = data.getDispatched();
  }
}

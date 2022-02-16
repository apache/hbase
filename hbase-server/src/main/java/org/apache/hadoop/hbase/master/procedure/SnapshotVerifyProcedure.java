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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.regionserver.SnapshotVerifyCallable;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotVerifyParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotVerifyProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 *  A remote procedure which is used to send verify snapshot request to region server.
 */
@InterfaceAudience.Private
public class SnapshotVerifyProcedure
    extends ServerRemoteProcedure implements TableProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotVerifyProcedure.class);

  private SnapshotDescription snapshot;
  private RegionInfo region;

  private RetryCounter retryCounter;

  public SnapshotVerifyProcedure() {}

  public SnapshotVerifyProcedure(SnapshotDescription snapshot, RegionInfo region) {
    this.snapshot = snapshot;
    this.region = region;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) {
    // nothing to rollback
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected synchronized void complete(MasterProcedureEnv env, Throwable error) {
    try {
      if (error != null) {
        if (error instanceof RemoteProcedureException) {
          // remote operation failed
          Throwable remoteEx = unwrapRemoteProcedureException((RemoteProcedureException) error);
          if (remoteEx instanceof CorruptedSnapshotException) {
            // snapshot is corrupted, will touch a flag file and finish the procedure
            succ = true;
            SnapshotProcedure parent = env.getMasterServices().getMasterProcedureExecutor()
              .getProcedure(SnapshotProcedure.class, getParentProcId());
            if (parent != null) {
              parent.markSnapshotCorrupted();
            }
          } else {
            // unexpected exception in remote server, will retry on other servers
            succ = false;
          }
        } else {
          // the mostly like thing is that remote call failed, will retry on other servers
          succ = false;
        }
      } else {
        // remote operation finished without error
        succ = true;
      }
    } catch (IOException e) {
      // if we can't create the flag file, then mark the current procedure as FAILED
      // and rollback the whole snapshot procedure stack.
      LOG.warn("Failed create corrupted snapshot flag file for snapshot={}, region={}",
        snapshot.getName(), region, e);
      setFailure("verify-snapshot", e);
    } finally {
      // release the worker
      env.getMasterServices().getSnapshotManager()
        .releaseSnapshotVerifyWorker(this, targetServer, env.getProcedureScheduler());
    }
  }

  // we will wrap remote exception into a RemoteProcedureException,
  // here we try to unwrap it
  private Throwable unwrapRemoteProcedureException(RemoteProcedureException e) {
    return e.getCause();
  }

  @Override
  protected synchronized Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    try {
      // if we've already known the snapshot is corrupted, then stop scheduling
      // the new procedures and the undispatched procedures
      if (!dispatched) {
        SnapshotProcedure parent = env.getMasterServices().getMasterProcedureExecutor()
          .getProcedure(SnapshotProcedure.class, getParentProcId());
        if (parent != null && parent.isSnapshotCorrupted()) {
          return null;
        }
      }
      // acquire a worker
      if (!dispatched && targetServer == null) {
        targetServer = env.getMasterServices()
          .getSnapshotManager().acquireSnapshotVerifyWorker(this);
      }
      // send remote request
      Procedure<MasterProcedureEnv>[] res = super.execute(env);
      // retry if necessary
      if (!dispatched) {
        // the mostly like thing is that a FailedRemoteDispatchException is thrown.
        // we need to retry on another remote server
        targetServer = null;
        throw new FailedRemoteDispatchException("Failed sent request");
      } else {
        // the request was successfully dispatched
        return res;
      }
    } catch (IOException e) {
      // there are some cases we need to retry:
      // 1. we can't get response from hdfs
      // 2. the remote server crashed
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn("Failed to get snapshot verify result , wait {} ms to retry", backoff, e);
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SnapshotVerifyProcedureStateData.Builder builder =
      SnapshotVerifyProcedureStateData.newBuilder();
    builder.setSnapshot(snapshot).setRegion(ProtobufUtil.toRegionInfo(region));
    if (targetServer != null) {
      builder.setTargetServer(ProtobufUtil.toServerName(targetServer));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SnapshotVerifyProcedureStateData data =
      serializer.deserialize(SnapshotVerifyProcedureStateData.class);
    this.snapshot = data.getSnapshot();
    this.region = ProtobufUtil.toRegionInfo(data.getRegion());
    if (data.hasTargetServer()) {
      this.targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    }
  }

  @Override
  protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getSimpleName())
      .append(", snapshot=").append(snapshot.getName());
    if (targetServer != null) {
      builder.append(", targetServer=").append(targetServer);
    }
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
    SnapshotVerifyParameter.Builder builder = SnapshotVerifyParameter.newBuilder();
    builder.setSnapshot(snapshot).setRegion(ProtobufUtil.toRegionInfo(region));
    return Optional.of(new RSProcedureDispatcher.ServerOperation(this, getProcId(),
      SnapshotVerifyCallable.class, builder.build().toByteArray()));
  }

  @Override
  public TableName getTableName() {
    return TableName.valueOf(snapshot.getTable());
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.SNAPSHOT;
  }

  public ServerName getServerName() {
    return targetServer;
  }
}

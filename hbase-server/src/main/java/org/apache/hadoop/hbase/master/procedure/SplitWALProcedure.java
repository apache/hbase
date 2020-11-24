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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.SplitWALManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * The procedure is to split a WAL. It will get an available region server and
 * schedule a {@link SplitWALRemoteProcedure} to actually send the request to region
 * server to split this WAL.
 * It also check if the split wal task really succeed. If the WAL still exists, it will
 * schedule another region server to split this WAL.
 */
@InterfaceAudience.Private
public class SplitWALProcedure
    extends StateMachineProcedure<MasterProcedureEnv, MasterProcedureProtos.SplitWALState>
    implements ServerProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(SplitWALProcedure.class);
  private String walPath;
  private ServerName worker;
  private ServerName crashedServer;
  private RetryCounter retryCounter;

  public SplitWALProcedure() {
  }

  public SplitWALProcedure(String walPath, ServerName crashedServer) {
    this.walPath = walPath;
    this.crashedServer = crashedServer;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, MasterProcedureProtos.SplitWALState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    SplitWALManager splitWALManager = env.getMasterServices().getSplitWALManager();
    switch (state) {
      case ACQUIRE_SPLIT_WAL_WORKER:
        worker = splitWALManager.acquireSplitWALWorker(this);
        setNextState(MasterProcedureProtos.SplitWALState.DISPATCH_WAL_TO_WORKER);
        return Flow.HAS_MORE_STATE;
      case DISPATCH_WAL_TO_WORKER:
        assert worker != null;
        addChildProcedure(new SplitWALRemoteProcedure(worker, crashedServer, walPath));
        setNextState(MasterProcedureProtos.SplitWALState.RELEASE_SPLIT_WORKER);
        return Flow.HAS_MORE_STATE;
      case RELEASE_SPLIT_WORKER:
        boolean finished;
        try {
          finished = splitWALManager.isSplitWALFinished(walPath);
        } catch (IOException ioe) {
          if (retryCounter == null) {
            retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
          }
          long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
          LOG.warn("Failed to check whether splitting wal {} success, wait {} seconds to retry",
            walPath, backoff / 1000, ioe);
          setTimeout(Math.toIntExact(backoff));
          setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
          skipPersistence();
          throw new ProcedureSuspendedException();
        }
        splitWALManager.releaseSplitWALWorker(worker, env.getProcedureScheduler());
        if (!finished) {
          LOG.warn("Failed to split wal {} by server {}, retry...", walPath, worker);
          setNextState(MasterProcedureProtos.SplitWALState.ACQUIRE_SPLIT_WAL_WORKER);
          return Flow.HAS_MORE_STATE;
        }
        ServerCrashProcedure.updateProgress(env, getParentProcId());
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env,
      MasterProcedureProtos.SplitWALState splitOneWalState)
      throws IOException, InterruptedException {
    if (splitOneWalState == getInitialState()) {
      return;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  protected MasterProcedureProtos.SplitWALState getState(int stateId) {
    return MasterProcedureProtos.SplitWALState.forNumber(stateId);
  }

  @Override
  protected int getStateId(MasterProcedureProtos.SplitWALState state) {
    return state.getNumber();
  }

  @Override
  protected MasterProcedureProtos.SplitWALState getInitialState() {
    return MasterProcedureProtos.SplitWALState.ACQUIRE_SPLIT_WAL_WORKER;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    MasterProcedureProtos.SplitWALData.Builder builder =
        MasterProcedureProtos.SplitWALData.newBuilder();
    builder.setWalPath(walPath).setCrashedServer(ProtobufUtil.toServerName(crashedServer));
    if (worker != null) {
      builder.setWorker(ProtobufUtil.toServerName(worker));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    MasterProcedureProtos.SplitWALData data =
        serializer.deserialize(MasterProcedureProtos.SplitWALData.class);
    walPath = data.getWalPath();
    crashedServer = ProtobufUtil.toServerName(data.getCrashedServer());
    if (data.hasWorker()) {
      worker = ProtobufUtil.toServerName(data.getWorker());
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  public String getWAL() {
    return walPath;
  }

  public ServerName getWorker(){
    return worker;
  }

  @Override
  public ServerName getServerName() {
    return this.crashedServer;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return AbstractFSWALProvider.isMetaFile(new Path(walPath));
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.SPLIT_WAL;
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env){
    if (worker != null) {
      if (env != null && env.getMasterServices() != null &&
          env.getMasterServices().getSplitWALManager() != null) {
        env.getMasterServices().getSplitWALManager().addUsedSplitWALWorker(worker);
      }
    }
  }

  @Override protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getProcName());
    if (this.worker != null) {
      builder.append(", worker=");
      builder.append(this.worker);
    }
    if (this.retryCounter != null) {
      builder.append(", retry=");
      builder.append(this.retryCounter);
    }
  }

  @Override public String getProcName() {
    return getClass().getSimpleName() + " " + getWALNameFromStrPath(getWAL());
  }

  /**
   * @return Return the WAL filename when given a Path-as-a-string; i.e. return the last path
   *   component only.
   */
  static String getWALNameFromStrPath(String path) {
    int slashIndex = path.lastIndexOf('/');
    return slashIndex != -1? path.substring(slashIndex + 1): path;
  }
}

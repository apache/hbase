/**
 *
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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.SplitWALCallable;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
/**
 * A remote procedure which is used to send split WAL request to region server.
 * it will return null if the task is succeed or return a DoNotRetryIOException
 * {@link SplitWALProcedure} will help handle the situation that encounter
 * DoNotRetryIOException. Otherwise it will retry until succeed.
 */
@InterfaceAudience.Private
public class SplitWALRemoteProcedure extends ServerRemoteProcedure
    implements ServerProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(SplitWALRemoteProcedure.class);
  private String walPath;
  private ServerName crashedServer;

  public SplitWALRemoteProcedure() {
  }

  public SplitWALRemoteProcedure(ServerName worker, ServerName crashedServer, String wal) {
    this.targetServer = worker;
    this.crashedServer = crashedServer;
    this.walPath = wal;
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
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.SplitWALRemoteData.Builder builder =
        MasterProcedureProtos.SplitWALRemoteData.newBuilder();
    builder.setWalPath(walPath).setWorker(ProtobufUtil.toServerName(targetServer))
        .setCrashedServer(ProtobufUtil.toServerName(crashedServer));
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.SplitWALRemoteData data =
        serializer.deserialize(MasterProcedureProtos.SplitWALRemoteData.class);
    walPath = data.getWalPath();
    targetServer = ProtobufUtil.toServerName(data.getWorker());
    crashedServer = ProtobufUtil.toServerName(data.getCrashedServer());
  }

  @Override
  public RemoteProcedureDispatcher.RemoteOperation remoteCallBuild(MasterProcedureEnv env,
      ServerName serverName) {
    return new RSProcedureDispatcher.ServerOperation(this, getProcId(), SplitWALCallable.class,
        MasterProcedureProtos.SplitWALParameter.newBuilder().setWalPath(walPath).build()
            .toByteArray());
  }

  @Override
  protected void complete(MasterProcedureEnv env, Throwable error) {
    if (error == null) {
      LOG.info("split WAL {} on {} succeeded", walPath, targetServer);
      try {
        env.getMasterServices().getSplitWALManager().deleteSplitWAL(walPath);
      } catch (IOException e) {
        LOG.warn("remove WAL {} failed, ignore...", walPath, e);
      }
      succ = true;
    } else {
      if (error instanceof DoNotRetryIOException) {
        LOG.warn("WAL split task of {} send to a wrong server {}, will retry on another server",
          walPath, targetServer, error);
        succ = true;
      } else {
        LOG.warn("split WAL {} failed, retry...", walPath, error);
        succ = false;
      }
    }
  }

  public String getWAL() {
    return this.walPath;
  }

  @Override
  public ServerName getServerName() {
    // return the crashed server is to use the queue of root ServerCrashProcedure
    return this.crashedServer;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return AbstractFSWALProvider.isMetaFile(new Path(walPath));
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.SPLIT_WAL_REMOTE;
  }
}

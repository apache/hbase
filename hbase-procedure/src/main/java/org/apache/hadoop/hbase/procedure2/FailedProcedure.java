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
package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@InterfaceAudience.Private
public class FailedProcedure<TEnvironment> extends Procedure<TEnvironment> {

  private String procName;

  public FailedProcedure() {
  }

  public FailedProcedure(long procId, String procName, User owner, NonceKey nonceKey,
      IOException exception) {
    this.procName = procName;
    setProcId(procId);
    setState(ProcedureState.ROLLEDBACK);
    setOwner(owner);
    setNonceKey(nonceKey);
    long currentTime = EnvironmentEdgeManager.currentTime();
    setSubmittedTime(currentTime);
    setLastUpdate(currentTime);
    setFailure(Objects.toString(exception.getMessage(), ""), exception);
  }

  @Override
  public String getProcName() {
    return procName;
  }

  @Override
  protected Procedure<TEnvironment>[] execute(TEnvironment env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void rollback(TEnvironment env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(TEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
  }
}

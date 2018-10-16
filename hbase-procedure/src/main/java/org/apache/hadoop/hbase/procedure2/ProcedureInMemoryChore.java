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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Special procedure used as a chore.
 * Instead of bringing the Chore class in (dependencies reason),
 * we reuse the executor timeout thread for this special case.
 *
 * The assumption is that procedure is used as hook to dispatch other procedures
 * or trigger some cleanups. It does not store state in the ProcedureStore.
 * this is just for in-memory chore executions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ProcedureInMemoryChore<TEnvironment> extends Procedure<TEnvironment> {
  protected ProcedureInMemoryChore(final int timeoutMsec) {
    setTimeout(timeoutMsec);
  }

  protected abstract void periodicExecute(final TEnvironment env);

  @Override
  protected Procedure<TEnvironment>[] execute(final TEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void rollback(final TEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(final TEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
  }
}

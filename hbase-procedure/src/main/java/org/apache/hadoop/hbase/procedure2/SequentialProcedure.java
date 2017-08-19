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
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.SequentialProcedureData;

/**
 * A SequentialProcedure describes one step in a procedure chain:
 * <pre>
 *   -&gt; Step 1 -&gt; Step 2 -&gt; Step 3
 * </pre>
 * The main difference from a base Procedure is that the execute() of a
 * SequentialProcedure will be called only once; there will be no second
 * execute() call once the children are finished. which means once the child
 * of a SequentialProcedure are completed the SequentialProcedure is completed too.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class SequentialProcedure<TEnvironment> extends Procedure<TEnvironment> {
  private boolean executed = false;

  @Override
  protected Procedure[] doExecute(final TEnvironment env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    updateTimestamp();
    try {
      Procedure[] children = !executed ? execute(env) : null;
      executed = !executed;
      return children;
    } finally {
      updateTimestamp();
    }
  }

  @Override
  protected void doRollback(final TEnvironment env)
      throws IOException, InterruptedException {
    updateTimestamp();
    if (executed) {
      try {
        rollback(env);
        executed = !executed;
      } finally {
        updateTimestamp();
      }
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    SequentialProcedureData.Builder data = SequentialProcedureData.newBuilder();
    data.setExecuted(executed);
    serializer.serialize(data.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    SequentialProcedureData data = serializer.deserialize(SequentialProcedureData.class);
    executed = data.getExecuted();
  }
}

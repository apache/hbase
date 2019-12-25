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
package org.apache.hadoop.hbase.procedure2.store.region;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;

import org.apache.hbase.thirdparty.com.google.protobuf.Int64Value;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

public class RegionProcedureStoreTestProcedure extends NoopProcedure<Void> {
  private static long SEQ_ID = 0;

  public RegionProcedureStoreTestProcedure() {
    setProcId(++SEQ_ID);
  }

  @Override
  protected Procedure<Void>[] execute(Void env) {
    return null;
  }

  @Override
  protected void rollback(Void env) {
  }

  @Override
  protected boolean abort(Void env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    long procId = getProcId();
    if (procId % 2 == 0) {
      Int64Value.Builder builder = Int64Value.newBuilder().setValue(procId);
      serializer.serialize(builder.build());
    }
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    long procId = getProcId();
    if (procId % 2 == 0) {
      Int64Value value = serializer.deserialize(Int64Value.class);
      assertEquals(procId, value.getValue());
    }
  }

  public void setParent(Procedure<?> proc) {
    super.setParentProcId(proc.getProcId());
  }

  public void finish() {
    setState(ProcedureState.SUCCESS);
  }
}
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
package org.apache.hadoop.hbase.procedure2.store;

import java.io.IOException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * when loading we will iterator the procedures twice, so use this class to cache the deserialized
 * result to prevent deserializing multiple times.
 */
@InterfaceAudience.Private
public class ProtoAndProcedure {
  private final ProcedureProtos.Procedure proto;

  private Procedure<?> proc;

  public ProtoAndProcedure(ProcedureProtos.Procedure proto) {
    this.proto = proto;
  }

  public Procedure<?> getProcedure() throws IOException {
    if (proc == null) {
      proc = ProcedureUtil.convertToProcedure(proto);
    }
    return proc;
  }

  public ProcedureProtos.Procedure getProto() {
    return proto;
  }
}
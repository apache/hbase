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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufMessageConverter;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.util.JRubyFormat;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureDescriber {
  private ProcedureDescriber() {
  }

  private static Object parametersToObject(Procedure<?> proc) {
    try {
      ProcedureProtos.Procedure protoProc = ProcedureUtil.convertToProtoProcedure(proc);
      List<Object> parameters = protoProc.getStateMessageList().stream()
        .map((any) -> {
          try {
            return ProtobufMessageConverter.toJavaObject(any);
          } catch (InvalidProtocolBufferException e) {
            return e.toString();
          }
        }).collect(Collectors.toList());
      return parameters;
    } catch (IOException e) {
      return e.toString();
    }
  }

  public static String describe(Procedure<?> proc) {
    Map<String, Object> description = new LinkedHashMap<>();

    description.put("ID", proc.getProcId());
    description.put("PARENT_ID", proc.getParentProcId());
    description.put("STATE", proc.getState());
    description.put("OWNER", proc.getOwner());
    description.put("TYPE", proc.getProcName());
    description.put("START_TIME", new Date(proc.getSubmittedTime()));
    description.put("LAST_UPDATE", new Date(proc.getLastUpdate()));

    if (proc.isFailed()) {
      description.put("ERRORS",
          MasterProcedureUtil.unwrapRemoteIOException(proc).getMessage());
    }
    description.put("PARAMETERS", parametersToObject(proc));

    return JRubyFormat.print(description);
  }

  public static String describeParameters(Procedure<?> proc) {
    Object object = parametersToObject(proc);
    return JRubyFormat.print(object);
  }
}

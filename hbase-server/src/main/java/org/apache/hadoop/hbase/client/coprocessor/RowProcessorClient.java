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

package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.RowProcessorProtos.ProcessRequest;
import org.apache.hadoop.hbase.regionserver.RowProcessor;

import com.google.protobuf.Message;
/**
 * Convenience class that is used to make RowProcessorEndpoint invocations.
 * For example usage, refer TestRowProcessorEndpoint
 *
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class RowProcessorClient {
  public static <S extends Message, T extends Message>
  ProcessRequest getRowProcessorPB(RowProcessor<S,T> r)
      throws IOException {
    final ProcessRequest.Builder requestBuilder =
        ProcessRequest.newBuilder();
    requestBuilder.setRowProcessorClassName(r.getClass().getName());
    S s = r.getRequestData();
    if (s != null) {
      requestBuilder.setRowProcessorInitializerMessageName(s.getClass().getName());
      requestBuilder.setRowProcessorInitializerMessage(s.toByteString());
    }
    return requestBuilder.build();
  }
}

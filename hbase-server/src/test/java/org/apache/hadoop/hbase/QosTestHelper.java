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
package org.apache.hadoop.hbase;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.regionserver.AnnotationReadingPriorityFunction;
import org.apache.hadoop.hbase.security.User;

import static org.junit.Assert.assertEquals;

public class QosTestHelper {
  protected void checkMethod(Configuration conf, final String methodName, final int expected,
                             final AnnotationReadingPriorityFunction qosf) {
    checkMethod(conf, methodName, expected, qosf, null);
  }

  protected void checkMethod(Configuration conf, final String methodName, final int expected,
                             final AnnotationReadingPriorityFunction qosf, final Message param) {
    RPCProtos.RequestHeader.Builder builder = RPCProtos.RequestHeader.newBuilder();
    builder.setMethodName(methodName);
    assertEquals(methodName, expected, qosf.getPriority(builder.build(), param,
      User.createUserForTesting(conf, "someuser", new String[]{"somegroup"})));
  }
}

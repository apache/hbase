package org.apache.hadoop.hbase.regionserver;
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
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.Message;

/**
 * Basic test that qos function is sort of working; i.e. a change in method naming style
 * over in pb doesn't break it.
 */
@Category(SmallTests.class)
public class TestQosFunction {
  @Test
  public void testPriority() {
    Configuration conf = HBaseConfiguration.create();
    RSRpcServices rpcServices = Mockito.mock(RSRpcServices.class);
    when(rpcServices.getConfiguration()).thenReturn(conf);

    AnnotationReadingPriorityFunction qosFunction =
      new AnnotationReadingPriorityFunction(rpcServices, RSRpcServices.class);

    // Set method name in pb style with the method name capitalized.
    checkMethod("ReplicateWALEntry", HConstants.REPLICATION_QOS, qosFunction);
    // Set method name in pb style with the method name capitalized.
    checkMethod("OpenRegion", HConstants.ADMIN_QOS, qosFunction);
    // Check multi works.
    checkMethod("Multi", HConstants.NORMAL_QOS, qosFunction, MultiRequest.getDefaultInstance());
  }

  private void checkMethod(final String methodName, final int expected,
      final AnnotationReadingPriorityFunction qosf) {
    checkMethod(methodName, expected, qosf, null);
  }

  private void checkMethod(final String methodName, final int expected,
      final AnnotationReadingPriorityFunction qosf, final Message param) {
    RequestHeader.Builder builder = RequestHeader.newBuilder();
    builder.setMethodName(methodName);
    assertEquals(methodName, expected, qosf.getPriority(builder.build(), param));
  }
}
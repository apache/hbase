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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.ServiceDescriptor;

import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos;

@Category(SmallTests.class)
public class TestCoprocessorRpcUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorRpcUtils.class);

  @Test
  public void testServiceName() throws Exception {
    // verify that we de-namespace build in HBase rpc services
    ServiceDescriptor authService =
        AuthenticationProtos.AuthenticationService.getDescriptor();
    assertEquals(authService.getName(), CoprocessorRpcUtils.getServiceName(authService));

    // non-hbase rpc services should remain fully qualified
    ServiceDescriptor dummyService =
        DummyRegionServerEndpointProtos.DummyService.getDescriptor();
    assertEquals(dummyService.getFullName(), CoprocessorRpcUtils.getServiceName(dummyService));
  }
}

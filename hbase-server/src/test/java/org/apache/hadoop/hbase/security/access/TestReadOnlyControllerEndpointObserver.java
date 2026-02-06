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
package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

// Tests methods of Endpoint Observer which are implemented in ReadOnlyController,
// by mocking the coprocessor environment and dependencies.
@Category({ SecurityTests.class, SmallTests.class })
public class TestReadOnlyControllerEndpointObserver {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyControllerEndpointObserver.class);

  EndpointReadOnlyController endpointReadOnlyController;
  HBaseConfiguration readOnlyConf;

  // Region Server Coprocessor mocking variables.
  ObserverContext<? extends RegionCoprocessorEnvironment> ctx;
  Service service;
  String methodName;
  Message request;

  @Before
  public void setup() throws Exception {
    endpointReadOnlyController = new EndpointReadOnlyController();
    readOnlyConf = new HBaseConfiguration();
    readOnlyConf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);

    // mocking variables initialization
    ctx = mock(ObserverContext.class);
    service = mock(Service.class);
    methodName = "testMethod";
    request = mock(Message.class);

    // Linking the mocks
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreEndpointInvocationReadOnlyException() throws IOException {
    endpointReadOnlyController.onConfigurationChange(readOnlyConf);
    endpointReadOnlyController.preEndpointInvocation(ctx, service, methodName, request);
  }

  @Test
  public void testPreEndpointInvocationNoException() throws IOException {
    endpointReadOnlyController.preEndpointInvocation(ctx, service, methodName, request);
  }
}

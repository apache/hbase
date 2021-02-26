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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.TestAsyncAdminBase;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyService;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncCoprocessorEndpoint extends TestAsyncAdminBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncCoprocessorEndpoint.class);

  private static final FileNotFoundException WHAT_TO_THROW = new FileNotFoundException("/file.txt");
  private static final String DUMMY_VALUE = "val";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      ProtobufCoprocessorService.class.getName());
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      DummyRegionServerEndpoint.class.getName());
    TEST_UTIL.startMiniCluster(2);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @Test
  public void testMasterCoprocessorService() throws Exception {
    TestProtos.EchoRequestProto request =
        TestProtos.EchoRequestProto.newBuilder().setMessage("hello").build();
    TestProtos.EchoResponseProto response =
        admin
            .<TestRpcServiceProtos.TestProtobufRpcProto.Stub, TestProtos.EchoResponseProto>
                coprocessorService(TestRpcServiceProtos.TestProtobufRpcProto::newStub,
                  (s, c, done) -> s.echo(c, request, done)).get();
    assertEquals("hello", response.getMessage());
  }

  @Test
  public void testMasterCoprocessorError() throws Exception {
    TestProtos.EmptyRequestProto emptyRequest = TestProtos.EmptyRequestProto.getDefaultInstance();
    try {
      admin
          .<TestRpcServiceProtos.TestProtobufRpcProto.Stub, TestProtos.EmptyResponseProto>
              coprocessorService(TestRpcServiceProtos.TestProtobufRpcProto::newStub,
                (s, c, done) -> s.error(c, emptyRequest, done)).get();
      fail("Should have thrown an exception");
    } catch (Exception e) {
    }
  }

  @Test
  public void testRegionServerCoprocessorService() throws Exception {
    final ServerName serverName = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    DummyRegionServerEndpointProtos.DummyRequest request =
        DummyRegionServerEndpointProtos.DummyRequest.getDefaultInstance();
    DummyRegionServerEndpointProtos.DummyResponse response =
        admin
            .<DummyRegionServerEndpointProtos.DummyService.Stub,
                DummyRegionServerEndpointProtos.DummyResponse> coprocessorService(
              DummyRegionServerEndpointProtos.DummyService::newStub,
                  (s, c, done) -> s.dummyCall(c, request, done), serverName).get();
    assertEquals(DUMMY_VALUE, response.getValue());
  }

  @Test
  public void testRegionServerCoprocessorServiceError() throws Exception {
    final ServerName serverName = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    DummyRegionServerEndpointProtos.DummyRequest request =
        DummyRegionServerEndpointProtos.DummyRequest.getDefaultInstance();
    try {
      admin
          .<DummyRegionServerEndpointProtos.DummyService.Stub,
              DummyRegionServerEndpointProtos.DummyResponse> coprocessorService(
            DummyRegionServerEndpointProtos.DummyService::newStub,
                (s, c, done) -> s.dummyThrow(c, request, done), serverName).get();
      fail("Should have thrown an exception");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RetriesExhaustedException);
      assertTrue(e.getCause().getMessage().contains(WHAT_TO_THROW.getClass().getName().trim()));
    }
  }

  public static class DummyRegionServerEndpoint extends DummyService
          implements RegionServerCoprocessor {
    public DummyRegionServerEndpoint() {}

    @Override
    public Iterable<Service> getServices() {
      return Collections.singleton(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public void dummyCall(RpcController controller, DummyRequest request,
        RpcCallback<DummyResponse> callback) {
      callback.run(DummyResponse.newBuilder().setValue(DUMMY_VALUE).build());
    }

    @Override
    public void dummyThrow(RpcController controller,
        DummyRequest request,
        RpcCallback<DummyResponse> done) {
      CoprocessorRpcUtils.setControllerException(controller, WHAT_TO_THROW);
    }
  }
}

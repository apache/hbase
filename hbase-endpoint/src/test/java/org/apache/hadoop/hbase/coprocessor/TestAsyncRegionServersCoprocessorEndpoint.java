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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.TestAsyncAdminBase;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyRequest;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyResponse;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyService;

@RunWith(Parameterized.class)
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncRegionServersCoprocessorEndpoint extends TestAsyncAdminBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncRegionServersCoprocessorEndpoint.class);

  private static final FileNotFoundException WHAT_TO_THROW = new FileNotFoundException("/file.txt");
  private static final String DUMMY_VALUE = "val";
  private static final int NUM_SLAVES = 5;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      ProtobufCoprocessorService.class.getName());
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      DummyRegionServerEndpoint.class.getName());
    TEST_UTIL.startMiniCluster(NUM_SLAVES);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @Test
  public void testRegionServersCoprocessorService()
    throws ExecutionException, InterruptedException {
    final List<JVMClusterUtil.RegionServerThread> regionServerThreads =
      TEST_UTIL.getHBaseCluster().getRegionServerThreads();
    List<ServerName> serverNames = new ArrayList<>();
    regionServerThreads.forEach(t -> serverNames.add(t.getRegionServer().getServerName()));

    DummyRequest request = DummyRequest.getDefaultInstance();
    Map<ServerName, Object> resultMap =
      admin.<DummyService.Stub, DummyResponse> coprocessorService(DummyService::newStub,
        (s, c, done) -> s.dummyCall(c, request, done), serverNames).get();

    resultMap.forEach((k, v) -> {
      assertTrue(v instanceof DummyResponse);
      DummyResponse resp = (DummyResponse) v;
      assertEquals(DUMMY_VALUE, resp.getValue());
    });
  }

  @Test
  public void testRegionServerCoprocessorsServiceError()
    throws ExecutionException, InterruptedException {
    final List<JVMClusterUtil.RegionServerThread> regionServerThreads =
      TEST_UTIL.getHBaseCluster().getRegionServerThreads();
    List<ServerName> serverNames = new ArrayList<>();
    regionServerThreads.forEach(t -> serverNames.add(t.getRegionServer().getServerName()));

    DummyRequest request = DummyRequest.getDefaultInstance();
    Map<ServerName, Object> resultMap =
      admin.<DummyService.Stub, DummyResponse> coprocessorService(DummyService::newStub,
        (s, c, done) -> s.dummyThrow(c, request, done), serverNames).get();

    resultMap.forEach((k, v) -> {
      assertTrue(v instanceof RetriesExhaustedException);
      Throwable e = (Throwable) v;
      assertTrue(e.getMessage().contains(WHAT_TO_THROW.getClass().getName().trim()));
    });
  }

  public static class DummyRegionServerEndpoint extends DummyService
    implements RegionServerCoprocessor {
    @Override
    public Iterable<Service> getServices() {
      return Collections.singleton(this);
    }

    @Override
    public void dummyCall(RpcController controller, DummyRequest request,
      RpcCallback<DummyResponse> callback) {
      callback.run(DummyResponse.newBuilder().setValue(DUMMY_VALUE).build());
    }

    @Override
    public void dummyThrow(RpcController controller, DummyRequest request,
      RpcCallback<DummyResponse> done) {
      CoprocessorRpcUtils.setControllerException(controller, WHAT_TO_THROW);
    }
  }
}

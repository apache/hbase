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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyRequest;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyResponse;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyService;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestRegionServerCoprocessorEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerCoprocessorEndpoint.class);

  public static final FileNotFoundException WHAT_TO_THROW = new FileNotFoundException("/file.txt");
  private static HBaseTestingUtility TEST_UTIL = null;
  private static Configuration CONF = null;
  private static final String DUMMY_VALUE = "val";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    CONF = TEST_UTIL.getConfiguration();
    CONF.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      DummyRegionServerEndpoint.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testEndpoint() throws Exception {
    final ServerName serverName = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    final ServerRpcController controller = new ServerRpcController();
    final CoprocessorRpcUtils.BlockingRpcCallback<DummyRegionServerEndpointProtos.DummyResponse>
        rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
    DummyRegionServerEndpointProtos.DummyService service =
        ProtobufUtil.newServiceStub(DummyRegionServerEndpointProtos.DummyService.class,
          TEST_UTIL.getAdmin().coprocessorService(serverName));
    service.dummyCall(controller,
        DummyRegionServerEndpointProtos.DummyRequest.getDefaultInstance(), rpcCallback);
    assertEquals(DUMMY_VALUE, rpcCallback.get().getValue());
    if (controller.failedOnException()) {
      throw controller.getFailedOn();
    }
  }

  @Test
  public void testEndpointExceptions() throws Exception {
    final ServerName serverName = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    final ServerRpcController controller = new ServerRpcController();
    final CoprocessorRpcUtils.BlockingRpcCallback<DummyRegionServerEndpointProtos.DummyResponse>
        rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
    DummyRegionServerEndpointProtos.DummyService service =
        ProtobufUtil.newServiceStub(DummyRegionServerEndpointProtos.DummyService.class,
            TEST_UTIL.getAdmin().coprocessorService(serverName));
    service.dummyThrow(controller,
        DummyRegionServerEndpointProtos.DummyRequest.getDefaultInstance(), rpcCallback);
    assertEquals(null, rpcCallback.get());
    assertTrue(controller.failedOnException());
    assertEquals(WHAT_TO_THROW.getClass(), controller.getFailedOn().getCause().getClass());
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
    public void dummyThrow(RpcController controller,
        DummyRequest request,
        RpcCallback<DummyResponse> done) {
      CoprocessorRpcUtils.setControllerException(controller, WHAT_TO_THROW);
    }
  }
}

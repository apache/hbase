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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.DummyRegionServerEndpointProtos.DummyService;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

@Category(MediumTests.class)
public class TestRegionServerCoprocessorEndpoint {
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
    final BlockingRpcCallback<DummyRegionServerEndpointProtos.DummyResponse> rpcCallback =
        new BlockingRpcCallback<DummyRegionServerEndpointProtos.DummyResponse>();
    DummyRegionServerEndpointProtos.DummyService service =
        ProtobufUtil.newServiceStub(DummyRegionServerEndpointProtos.DummyService.class,
          new HBaseAdmin(CONF).coprocessorService(serverName));
    service.dummyCall(controller,
      DummyRegionServerEndpointProtos.DummyRequest.getDefaultInstance(), rpcCallback);
    assertEquals(DUMMY_VALUE, rpcCallback.get().getValue());
    if (controller.failedOnException()) {
      throw controller.getFailedOn();
    }
  }

  static class DummyRegionServerEndpoint extends DummyService implements Coprocessor, SingletonCoprocessorService {

    @Override
    public Service getService() {
      return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      // TODO Auto-generated method stub
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
      // TODO Auto-generated method stub
    }

    @Override
    public void dummyCall(RpcController controller, DummyRequest request,
        RpcCallback<DummyResponse> callback) {
      callback.run(DummyResponse.newBuilder().setValue(DUMMY_VALUE).build());
    }
  }
}

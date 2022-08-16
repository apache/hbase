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
package org.apache.hadoop.hbase.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.SimpleRpcServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ SecurityTests.class, LargeTests.class })
public class TestSecureIPC extends AbstractTestSecureIPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSecureIPC.class);

  @Parameters(name = "{index}: rpcClientImpl={0}, rpcServerImpl={1}")
  public static Collection<Object[]> parameters() {
    List<Object[]> params = new ArrayList<>();
    List<String> rpcClientImpls =
      Arrays.asList(BlockingRpcClient.class.getName(), NettyRpcClient.class.getName());
    List<String> rpcServerImpls =
      Arrays.asList(SimpleRpcServer.class.getName(), NettyRpcServer.class.getName());
    for (String rpcClientImpl : rpcClientImpls) {
      for (String rpcServerImpl : rpcServerImpls) {
        params.add(new Object[] { rpcClientImpl, rpcServerImpl });
      }
    }
    return params;
  }

  @Parameter(0)
  public String rpcClientImpl;

  @Parameter(1)
  public String rpcServerImpl;

  @BeforeClass
  public static void setUp() throws Exception {
    initKDCAndConf();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    stopKDC();
    TEST_UTIL.cleanupTestDir();
  }

  @Before
  public void setUpTest() throws Exception {
    setUpPrincipalAndConf();
    clientConf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientImpl);
    serverConf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl);
  }
}

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
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.SimpleRpcServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.provider.Arguments;

@Tag(SecurityTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: rpcClientImpl={0}, rpcServerImpl={1}")
public class TestSecureIPC extends AbstractTestSecureIPC {

  public static Stream<Arguments> parameters() {
    List<Arguments> params = new ArrayList<>();
    List<String> rpcClientImpls =
      Arrays.asList(BlockingRpcClient.class.getName(), NettyRpcClient.class.getName());
    List<String> rpcServerImpls =
      Arrays.asList(SimpleRpcServer.class.getName(), NettyRpcServer.class.getName());
    for (String rpcClientImpl : rpcClientImpls) {
      for (String rpcServerImpl : rpcServerImpls) {
        params.add(Arguments.of(rpcClientImpl, rpcServerImpl));
      }
    }
    return params.stream();
  }

  public String rpcClientImpl;

  public String rpcServerImpl;

  public TestSecureIPC(String rpcClientImpl, String rpcServerImpl) {
    this.rpcClientImpl = rpcClientImpl;
    this.rpcServerImpl = rpcServerImpl;
  }

  @BeforeAll
  public static void setUp() throws Exception {
    initKDCAndConf();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    stopKDC();
    TEST_UTIL.cleanupTestDir();
  }

  @BeforeEach
  public void setUpTest() throws Exception {
    setUpPrincipalAndConf();
    clientConf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientImpl);
    serverConf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl);
  }
}

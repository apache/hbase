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
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ipc.RpcCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestRpcCoprocessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcCoprocessor.class);

  public static class AuthorizationRpcObserver implements RpcCoprocessor, RpcObserver {
    final AtomicInteger ctPostAuthorization = new AtomicInteger(0);
    final AtomicInteger ctPreAuthorization = new AtomicInteger(0);
    String userName = null;

    @Override
    public Optional<RpcObserver> getRpcObserver() {
      return Optional.of(this);
    }

    @Override
    public void preAuthorizeConnection(ObserverContext<RpcCoprocessorEnvironment> ctx,
      RPCProtos.ConnectionHeader connectionHeader, InetAddress remoteAddr) throws IOException {
      ctPreAuthorization.incrementAndGet();
    }

    @Override
    public void postAuthorizeConnection(ObserverContext<RpcCoprocessorEnvironment> ctx,
      String userName, X509Certificate[] clientCertificateChain) throws IOException {
      ctPostAuthorization.incrementAndGet();
      this.userName = userName;
    }
  }

  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.RPC_COPROCESSOR_CONF_KEY, AuthorizationRpcObserver.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHooksCalledFromMaster() {
    RpcCoprocessorHost coprocHostMaster =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getRpcServer().getRpcCoprocessorHost();
    AuthorizationRpcObserver observer =
      coprocHostMaster.findCoprocessor(AuthorizationRpcObserver.class);
    assertEquals(2, observer.ctPreAuthorization.get());
    assertEquals(2, observer.ctPostAuthorization.get());
    assertEquals(System.getProperty("user.name"), observer.userName);
  }

  @Test
  public void testHooksCalledFromRegionServer() {
    RpcCoprocessorHost coprocHostRs =
      TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getRpcServer().getRpcCoprocessorHost();
    AuthorizationRpcObserver observer =
      coprocHostRs.findCoprocessor(AuthorizationRpcObserver.class);
    assertEquals(3, observer.ctPreAuthorization.get());
    assertEquals(3, observer.ctPostAuthorization.get());
    assertEquals(System.getProperty("user.name"), observer.userName);
  }
}

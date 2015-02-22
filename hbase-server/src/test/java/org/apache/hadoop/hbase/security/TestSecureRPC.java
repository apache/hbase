/**
 *
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

import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getSecuredConfiguration;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.isKerberosPropertySetted;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.TestDelayedRpc.TestDelayedImplementation;
import org.apache.hadoop.hbase.ipc.TestDelayedRpc.TestThread;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestDelayedRpcProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;

@Category({SecurityTests.class, SmallTests.class})
public class TestSecureRPC {
  public static RpcServerInterface rpcServer;
  /**
   * To run this test, we must specify the following system properties:
   *<p>
   * <b> hbase.regionserver.kerberos.principal </b>
   * <p>
   * <b> hbase.regionserver.keytab.file </b>
   */
  @Test
  public void testRpcCallWithEnabledKerberosSaslAuth() throws Exception {
    assumeTrue(isKerberosPropertySetted());
    String krbKeytab = getKeytabFileForTesting();
    String krbPrincipal = getPrincipalForTesting();

    Configuration cnf = new Configuration();
    cnf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(cnf);
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi, ugi2);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    Configuration conf = getSecuredConfiguration();

    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipal())
      .thenReturn(HBaseKerberosUtils.KRB_PRINCIPAL);
    SecurityInfo.addInfo("TestDelayedService", securityInfoMock);

    boolean delayReturnValue = false;
    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    TestDelayedImplementation instance = new TestDelayedImplementation(delayReturnValue);
    BlockingService service =
        TestDelayedRpcProtos.TestDelayedService.newReflectiveBlockingService(instance);

    rpcServer = new RpcServer(null, "testSecuredDelayedRpc",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
          isa, conf, new FifoRpcScheduler(conf, 1));
    rpcServer.start();
    RpcClient rpcClient = RpcClientFactory
        .createClient(conf, HConstants.DEFAULT_CLUSTER_ID.toString());
    try {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
          ServerName.valueOf(rpcServer.getListenerAddress().getHostName(),
              rpcServer.getListenerAddress().getPort(), System.currentTimeMillis()),
          User.getCurrent(), 1000);
      TestDelayedRpcProtos.TestDelayedService.BlockingInterface stub =
        TestDelayedRpcProtos.TestDelayedService.newBlockingStub(channel);
      List<Integer> results = new ArrayList<Integer>();
      TestThread th1 = new TestThread(stub, true, results);
      th1.start();
      Thread.sleep(100);
      th1.join();

      assertEquals(0xDEADBEEF, results.get(0).intValue());
    } finally {
      rpcClient.close();
    }
  }
}
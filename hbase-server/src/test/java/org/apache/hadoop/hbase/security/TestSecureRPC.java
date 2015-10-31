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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.TestDelayedRpc.TestDelayedImplementation;
import org.apache.hadoop.hbase.ipc.TestDelayedRpc.TestThread;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestDelayedRpcProtos;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;

@Category({ SecurityTests.class, SmallTests.class })
public class TestSecureRPC {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri()
      .getPath());

  private static MiniKdc KDC;

  private static String HOST = "localhost";

  private static String PRINCIPAL;

  @BeforeClass
  public static void setUp() throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    KDC = new MiniKdc(conf, new File(TEST_UTIL.getDataTestDir("kdc").toUri().getPath()));
    KDC.start();
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setKeytabFileForTesting(KEYTAB_FILE.getAbsolutePath());
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testRpc() throws Exception {
    testRpcCallWithEnabledKerberosSaslAuth(RpcClientImpl.class);
  }

  @Test
  public void testRpcWithInsecureFallback() throws Exception {
    testRpcFallbackToSimpleAuth(RpcClientImpl.class);
  }

  @Test
  public void testAsyncRpc() throws Exception {
    testRpcCallWithEnabledKerberosSaslAuth(AsyncRpcClient.class);
  }

  @Test
  public void testAsyncRpcWithInsecureFallback() throws Exception {
    testRpcFallbackToSimpleAuth(AsyncRpcClient.class);
  }

  private void testRpcCallWithEnabledKerberosSaslAuth(Class<? extends RpcClient> rpcImplClass)
      throws Exception {
    String krbKeytab = getKeytabFileForTesting();
    String krbPrincipal = getPrincipalForTesting();

    UserGroupInformation ugi = loginKerberosPrincipal(krbKeytab, krbPrincipal);
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi, ugi2);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    Configuration clientConf = getSecuredConfiguration();
    callRpcService(rpcImplClass, User.create(ugi2), clientConf, false);
  }

  private UserGroupInformation loginKerberosPrincipal(String krbKeytab, String krbPrincipal)
      throws Exception {
    Configuration cnf = new Configuration();
    cnf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(cnf);
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
    return UserGroupInformation.getLoginUser();
  }

  private void callRpcService(Class<? extends RpcClient> rpcImplClass, User clientUser,
                              Configuration clientConf, boolean allowInsecureFallback)
      throws Exception {
    Configuration clientConfCopy = new Configuration(clientConf);
    clientConfCopy.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcImplClass.getName());

    Configuration conf = getSecuredConfiguration();
    conf.setBoolean(RpcServer.FALLBACK_TO_INSECURE_CLIENT_AUTH, allowInsecureFallback);

    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipal())
        .thenReturn(HBaseKerberosUtils.KRB_PRINCIPAL);
    SecurityInfo.addInfo("TestDelayedService", securityInfoMock);

    boolean delayReturnValue = false;
    InetSocketAddress isa = new InetSocketAddress(HOST, 0);
    TestDelayedImplementation instance = new TestDelayedImplementation(delayReturnValue);
    BlockingService service =
        TestDelayedRpcProtos.TestDelayedService.newReflectiveBlockingService(instance);

    RpcServerInterface rpcServer =
        new RpcServer(null, "testSecuredDelayedRpc",
            Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)), isa,
            conf, new FifoRpcScheduler(conf, 1));
    rpcServer.start();
    RpcClient rpcClient =
        RpcClientFactory.createClient(clientConfCopy, HConstants.DEFAULT_CLUSTER_ID.toString());
    try {
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      BlockingRpcChannel channel =
          rpcClient.createBlockingRpcChannel(
            ServerName.valueOf(address.getHostName(), address.getPort(),
            System.currentTimeMillis()), clientUser, 5000);
      TestDelayedRpcProtos.TestDelayedService.BlockingInterface stub =
          TestDelayedRpcProtos.TestDelayedService.newBlockingStub(channel);
      List<Integer> results = new ArrayList<Integer>();
      TestThread th1 = new TestThread(stub, true, results);
      th1.start();
      th1.join();

      assertEquals(0xDEADBEEF, results.get(0).intValue());
    } finally {
      rpcClient.close();
      rpcServer.stop();
    }
  }

  public void testRpcFallbackToSimpleAuth(Class<? extends RpcClient> rpcImplClass) throws Exception {
    String krbKeytab = getKeytabFileForTesting();
    String krbPrincipal = getPrincipalForTesting();

    UserGroupInformation ugi = loginKerberosPrincipal(krbKeytab, krbPrincipal);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    String clientUsername = "testuser";
    UserGroupInformation clientUgi = UserGroupInformation.createUserForTesting(clientUsername,
        new String[]{clientUsername});

    // check that the client user is insecure
    assertNotSame(ugi, clientUgi);
    assertEquals(AuthenticationMethod.SIMPLE, clientUgi.getAuthenticationMethod());
    assertEquals(clientUsername, clientUgi.getUserName());

    Configuration clientConf = new Configuration();
    clientConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    callRpcService(rpcImplClass, User.create(clientUgi), clientConf, true);
  }
}
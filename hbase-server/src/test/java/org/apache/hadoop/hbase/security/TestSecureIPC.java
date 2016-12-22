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

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getSecuredConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import javax.security.sasl.SaslException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;

@RunWith(Parameterized.class)
@Category({ SecurityTests.class, SmallTests.class })
public class TestSecureIPC {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final File KEYTAB_FILE = new File(
      TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;

  String krbKeytab;
  String krbPrincipal;
  UserGroupInformation ugi;
  Configuration clientConf;
  Configuration serverConf;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Parameters(name = "{index}: rpcClientImpl={0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[]{BlockingRpcClient.class.getName()},
        new Object[]{NettyRpcClient.class.getName()});
  }

  @Parameter
  public String rpcClientImpl;

  @BeforeClass
  public static void setUp() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.cleanupTestDir();
  }

  @Before
  public void setUpTest() throws Exception {
    krbKeytab = getKeytabFileForTesting();
    krbPrincipal = getPrincipalForTesting();
    ugi = loginKerberosPrincipal(krbKeytab, krbPrincipal);
    clientConf = getSecuredConfiguration();
    clientConf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientImpl);
    serverConf = getSecuredConfiguration();
  }

  @Test
  public void testRpcCallWithEnabledKerberosSaslAuth() throws Exception {
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi, ugi2);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    callRpcService(User.create(ugi2));
  }

  @Test
  public void testRpcFallbackToSimpleAuth() throws Exception {
    String clientUsername = "testuser";
    UserGroupInformation clientUgi = UserGroupInformation.createUserForTesting(clientUsername,
      new String[] { clientUsername });

    // check that the client user is insecure
    assertNotSame(ugi, clientUgi);
    assertEquals(AuthenticationMethod.SIMPLE, clientUgi.getAuthenticationMethod());
    assertEquals(clientUsername, clientUgi.getUserName());

    clientConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    serverConf.setBoolean(RpcServer.FALLBACK_TO_INSECURE_CLIENT_AUTH, true);
    callRpcService(User.create(clientUgi));
  }

  void setRpcProtection(String clientProtection, String serverProtection) {
    clientConf.set("hbase.rpc.protection", clientProtection);
    serverConf.set("hbase.rpc.protection", serverProtection);
  }

  /**
   * Test various combinations of Server and Client qops.
   * @throws Exception
   */
  @Test
  public void testSaslWithCommonQop() throws Exception {
    setRpcProtection("privacy,authentication", "authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("authentication", "privacy,authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("integrity,authentication", "privacy,authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("integrity,authentication", "integrity,authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("privacy,authentication", "privacy,authentication");
    callRpcService(User.create(ugi));
  }

  @Test
  public void testSaslNoCommonQop() throws Exception {
    exception.expect(SaslException.class);
    exception.expectMessage("No common protection layer between client and server");
    setRpcProtection("integrity", "privacy");
    callRpcService(User.create(ugi));
  }

  /**
   * Test sasl encryption with Crypto AES.
   * @throws Exception
   */
  @Test
  public void testSaslWithCryptoAES() throws Exception {
    setRpcProtection("privacy", "privacy");
    setCryptoAES("true", "true");
    callRpcService(User.create(ugi));
  }

  /**
   * Test various combinations of Server and Client configuration for Crypto AES.
   * @throws Exception
   */
  @Test
  public void testDifferentConfWithCryptoAES() throws Exception {
    setRpcProtection("privacy", "privacy");

    setCryptoAES("false", "true");
    callRpcService(User.create(ugi));

    setCryptoAES("true", "false");
    try {
      callRpcService(User.create(ugi));
      fail("The exception should be thrown out for the rpc timeout.");
    } catch (Exception e) {
      // ignore the expected exception
    }
  }

  void setCryptoAES(String clientCryptoAES, String serverCryptoAES) {
    clientConf.set("hbase.rpc.crypto.encryption.aes.enabled", clientCryptoAES);
    serverConf.set("hbase.rpc.crypto.encryption.aes.enabled", serverCryptoAES);
  }

  private UserGroupInformation loginKerberosPrincipal(String krbKeytab, String krbPrincipal)
      throws Exception {
    Configuration cnf = new Configuration();
    cnf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(cnf);
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
    return UserGroupInformation.getLoginUser();
  }

  /**
   * Sets up a RPC Server and a Client. Does a RPC checks the result. If an exception is thrown from
   * the stub, this function will throw root cause of that exception.
   */
  private void callRpcService(User clientUser) throws Exception {
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipal())
        .thenReturn(HBaseKerberosUtils.KRB_PRINCIPAL);
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);

    InetSocketAddress isa = new InetSocketAddress(HOST, 0);

    RpcServerInterface rpcServer = RpcServerFactory.createRpcServer(null, "AbstractTestSecureIPC",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface((BlockingService) SERVICE, null)), isa,
        serverConf, new FifoRpcScheduler(serverConf, 1));
    rpcServer.start();
    try (RpcClient rpcClient = RpcClientFactory.createClient(clientConf,
      HConstants.DEFAULT_CLUSTER_ID.toString())) {
      BlockingInterface stub = newBlockingStub(rpcClient, rpcServer.getListenerAddress(),
        clientUser);
      TestThread th1 = new TestThread(stub);
      final Throwable exception[] = new Throwable[1];
      Collections.synchronizedList(new ArrayList<Throwable>());
      Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread th, Throwable ex) {
          exception[0] = ex;
        }
      };
      th1.setUncaughtExceptionHandler(exceptionHandler);
      th1.start();
      th1.join();
      if (exception[0] != null) {
        // throw root cause.
        while (exception[0].getCause() != null) {
          exception[0] = exception[0].getCause();
        }
        throw (Exception) exception[0];
      }
    } finally {
      rpcServer.stop();
    }
  }

  public static class TestThread extends Thread {
    private final BlockingInterface stub;

    public TestThread(BlockingInterface stub) {
      this.stub = stub;
    }

    @Override
    public void run() {
      try {
        int[] messageSize = new int[] { 100, 1000, 10000 };
        for (int i = 0; i < messageSize.length; i++) {
          String input = RandomStringUtils.random(messageSize[i]);
          String result = stub
              .echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage(input).build())
              .getMessage();
          assertEquals(input, result);
        }
      } catch (org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
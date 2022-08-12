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

import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED;
import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.HBASE_SERVER_NETTY_TLS_ENABLED;
import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.setSecuredConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContext;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

@Category({ SecurityTests.class, LargeTests.class })
public class TestTlsWithKerberos {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTlsWithKerberos.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final File KEYTAB_FILE =
    new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  private static MiniKdc KDC;
  private static final String HOST = "localhost";
  private static String PRINCIPAL;
  private static final String RPC_CLIENT_IMPL = NettyRpcClient.class.getName();
  private static final String RPC_SERVER_IMPL = NettyRpcServer.class.getName();

  private String krbKeytab;
  private String krbPrincipal;
  private UserGroupInformation ugi;
  private Configuration clientConf;
  private Configuration serverConf;
  private static X509TestContext x509TestContext;

  @BeforeClass
  public static void setUp() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    x509TestContext = X509TestContext.newBuilder()
      .setTempDir(new File(TEST_UTIL.getDataTestDir().toUri().getPath()))
      .setKeyStorePassword("Pa$$word").setKeyStoreKeyType(X509KeyType.RSA)
      .setTrustStoreKeyType(X509KeyType.RSA).setTrustStorePassword("Pa$$word").build();
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
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
    clientConf = HBaseConfiguration.create(x509TestContext.getConf());
    setSecuredConfiguration(clientConf);
    clientConf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, RPC_CLIENT_IMPL);
    serverConf = HBaseConfiguration.create(x509TestContext.getConf());
    setSecuredConfiguration(serverConf);
    serverConf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, RPC_SERVER_IMPL);
  }

  @Test
  public void testNoPlaintext() throws Exception {
    setRpcProtection("authentication", "authentication");
    setTLSEncryption(true, false, true);
    callRpcService(User.create(ugi));
  }

  @Test
  public void testRejectPlaintext() {
    setRpcProtection("authentication", "authentication");
    setTLSEncryption(true, false, false);
    Assert.assertThrows(ConnectionClosedException.class, () -> callRpcService(User.create(ugi)));
  }

  @Test
  public void testAcceptPlaintext() throws Exception {
    setRpcProtection("authentication", "authentication");
    setTLSEncryption(true, true, false);
    callRpcService(User.create(ugi));
  }

  void setTLSEncryption(Boolean server, Boolean acceptPlaintext, Boolean client) {
    serverConf.set(HBASE_SERVER_NETTY_TLS_ENABLED, server.toString());
    serverConf.set(HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, acceptPlaintext.toString());
    clientConf.set(HBASE_CLIENT_NETTY_TLS_ENABLED, client.toString());
  }

  void setRpcProtection(String clientProtection, String serverProtection) {
    clientConf.set("hbase.rpc.protection", clientProtection);
    serverConf.set("hbase.rpc.protection", serverProtection);
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
      Lists
        .newArrayList(new RpcServer.BlockingServiceAndInterface((BlockingService) SERVICE, null)),
      isa, serverConf, new FifoRpcScheduler(serverConf, 1));
    rpcServer.start();
    try (RpcClient rpcClient =
      RpcClientFactory.createClient(clientConf, HConstants.DEFAULT_CLUSTER_ID.toString())) {
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
        newBlockingStub(rpcClient, rpcServer.getListenerAddress(), clientUser);
      TestSecureIPC.TestThread th = new TestSecureIPC.TestThread(stub);
      AtomicReference<Throwable> exception = new AtomicReference<>();
      Collections.synchronizedList(new ArrayList<Throwable>());
      Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread th, Throwable ex) {
          exception.set(ex);
        }
      };
      th.setUncaughtExceptionHandler(exceptionHandler);
      th.start();
      th.join();
      if (exception.get() != null) {
        // throw root cause.
        while (exception.get().getCause() != null) {
          exception.set(exception.get().getCause());
        }
        throw (Exception) exception.get();
      }
    } finally {
      rpcServer.stop();
    }
  }
}

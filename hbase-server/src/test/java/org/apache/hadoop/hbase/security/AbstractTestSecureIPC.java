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

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.loginKerberosPrincipal;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.setSecuredConfiguration;
import static org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders.SELECTOR_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.ipc.FallbackDisallowedException;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.security.provider.AuthenticationProviderSelector;
import org.apache.hadoop.hbase.security.provider.BuiltInProviderSelector;
import org.apache.hadoop.hbase.security.provider.SaslAuthMethod;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

public class AbstractTestSecureIPC {

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static final File KEYTAB_FILE =
    new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  protected static MiniKdc KDC;
  protected static String HOST = "localhost";
  protected static String PRINCIPAL;

  protected String krbKeytab;
  protected String krbPrincipal;
  protected UserGroupInformation ugi;
  protected Configuration clientConf;
  protected Configuration serverConf;

  protected static void initKDCAndConf() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    // set a smaller timeout and retry to speed up tests
    TEST_UTIL.getConfiguration().setInt(RpcClient.SOCKET_TIMEOUT_READ, 2000);
    TEST_UTIL.getConfiguration().setInt("hbase.security.relogin.maxretries", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.security.relogin.maxbackoff", 100);
  }

  protected static void stopKDC() {
    if (KDC != null) {
      KDC.stop();
    }
  }

  protected final void setUpPrincipalAndConf() throws Exception {
    krbKeytab = getKeytabFileForTesting();
    krbPrincipal = getPrincipalForTesting();
    ugi = loginKerberosPrincipal(krbKeytab, krbPrincipal);
    clientConf = new Configuration(TEST_UTIL.getConfiguration());
    setSecuredConfiguration(clientConf);
    serverConf = new Configuration(TEST_UTIL.getConfiguration());
    setSecuredConfiguration(serverConf);
  }

  @Test
  public void testRpcCallWithEnabledKerberosSaslAuth() throws Exception {
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi2, ugi);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    callRpcService(User.create(ugi2));
  }

  @Test
  public void testRpcCallWithEnabledKerberosSaslAuthCanonicalHostname() throws Exception {
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi2, ugi);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    enableCanonicalHostnameTesting(clientConf, "localhost");
    clientConf.setBoolean(
      SecurityConstants.UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS, false);
    clientConf.set(HBaseKerberosUtils.KRB_PRINCIPAL, "hbase/_HOST@" + KDC.getRealm());

    callRpcService(User.create(ugi2));
  }

  @Test
  public void testRpcCallWithEnabledKerberosSaslAuthNoCanonicalHostname() throws Exception {
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi2, ugi);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    enableCanonicalHostnameTesting(clientConf, "127.0.0.1");
    clientConf
      .setBoolean(SecurityConstants.UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS, true);
    clientConf.set(HBaseKerberosUtils.KRB_PRINCIPAL, "hbase/_HOST@" + KDC.getRealm());

    callRpcService(User.create(ugi2));
  }

  private static void enableCanonicalHostnameTesting(Configuration conf, String canonicalHostname) {
    conf.setClass(SELECTOR_KEY, CanonicalHostnameTestingAuthenticationProviderSelector.class,
      AuthenticationProviderSelector.class);
    conf.set(CanonicalHostnameTestingAuthenticationProviderSelector.CANONICAL_HOST_NAME_KEY,
      canonicalHostname);
  }

  public static class CanonicalHostnameTestingAuthenticationProviderSelector
    extends BuiltInProviderSelector {
    private static final String CANONICAL_HOST_NAME_KEY =
      "CanonicalHostnameTestingAuthenticationProviderSelector.canonicalHostName";

    @Override
    public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>>
      selectProvider(String clusterId, User user) {
      final Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> pair =
        super.selectProvider(clusterId, user);
      pair.setFirst(createCanonicalHostNameTestingProvider(pair.getFirst()));
      return pair;
    }

    SaslClientAuthenticationProvider
      createCanonicalHostNameTestingProvider(SaslClientAuthenticationProvider delegate) {
      return new SaslClientAuthenticationProvider() {
        @Override
        public SaslClient createClient(Configuration conf, InetAddress serverAddr,
          String serverPrincipal, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
          Map<String, String> saslProps) throws IOException {
          final String s = conf.get(CANONICAL_HOST_NAME_KEY);
          if (s != null) {
            try {
              final Field canonicalHostName =
                InetAddress.class.getDeclaredField("canonicalHostName");
              canonicalHostName.setAccessible(true);
              canonicalHostName.set(serverAddr, s);
            } catch (NoSuchFieldException | IllegalAccessException e) {
              throw new RuntimeException(e);
            }
          }

          return delegate.createClient(conf, serverAddr, serverPrincipal, token, fallbackAllowed,
            saslProps);
        }

        @Override
        public UserInformation getUserInfo(User user) {
          return delegate.getUserInfo(user);
        }

        @Override
        public UserGroupInformation getRealUser(User ugi) {
          return delegate.getRealUser(ugi);
        }

        @Override
        public boolean canRetry() {
          return delegate.canRetry();
        }

        @Override
        public void relogin() throws IOException {
          delegate.relogin();
        }

        @Override
        public SaslAuthMethod getSaslAuthMethod() {
          return delegate.getSaslAuthMethod();
        }

        @Override
        public String getTokenKind() {
          return delegate.getTokenKind();
        }
      };
    }
  }

  @Test
  public void testRpcServerFallbackToSimpleAuth() throws Exception {
    String clientUsername = "testuser";
    UserGroupInformation clientUgi =
      UserGroupInformation.createUserForTesting(clientUsername, new String[] { clientUsername });

    // check that the client user is insecure
    assertNotSame(ugi, clientUgi);
    assertEquals(AuthenticationMethod.SIMPLE, clientUgi.getAuthenticationMethod());
    assertEquals(clientUsername, clientUgi.getUserName());

    clientConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    serverConf.setBoolean(RpcServer.FALLBACK_TO_INSECURE_CLIENT_AUTH, true);
    callRpcService(User.create(clientUgi));
  }

  @Test
  public void testRpcServerDisallowFallbackToSimpleAuth() throws Exception {
    String clientUsername = "testuser";
    UserGroupInformation clientUgi =
      UserGroupInformation.createUserForTesting(clientUsername, new String[] { clientUsername });

    // check that the client user is insecure
    assertNotSame(ugi, clientUgi);
    assertEquals(AuthenticationMethod.SIMPLE, clientUgi.getAuthenticationMethod());
    assertEquals(clientUsername, clientUgi.getUserName());

    clientConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    serverConf.setBoolean(RpcServer.FALLBACK_TO_INSECURE_CLIENT_AUTH, false);
    IOException error =
      assertThrows(IOException.class, () -> callRpcService(User.create(clientUgi)));
    // server just closes the connection, so we could get broken pipe, or EOF, or connection closed
    if (error.getMessage() == null || !error.getMessage().contains("Broken pipe")) {
      assertThat(error,
        either(instanceOf(EOFException.class)).or(instanceOf(ConnectionClosedException.class)));
    }
  }

  @Test
  public void testRpcClientFallbackToSimpleAuth() throws Exception {
    String serverUsername = "testuser";
    UserGroupInformation serverUgi =
      UserGroupInformation.createUserForTesting(serverUsername, new String[] { serverUsername });
    // check that the server user is insecure
    assertNotSame(ugi, serverUgi);
    assertEquals(AuthenticationMethod.SIMPLE, serverUgi.getAuthenticationMethod());
    assertEquals(serverUsername, serverUgi.getUserName());

    serverConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    clientConf.setBoolean(RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, true);
    callRpcService(User.create(serverUgi), User.create(ugi));
  }

  @Test
  public void testRpcClientDisallowFallbackToSimpleAuth() throws Exception {
    String serverUsername = "testuser";
    UserGroupInformation serverUgi =
      UserGroupInformation.createUserForTesting(serverUsername, new String[] { serverUsername });
    // check that the server user is insecure
    assertNotSame(ugi, serverUgi);
    assertEquals(AuthenticationMethod.SIMPLE, serverUgi.getAuthenticationMethod());
    assertEquals(serverUsername, serverUgi.getUserName());

    serverConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    clientConf.setBoolean(RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, false);
    assertThrows(FallbackDisallowedException.class,
      () -> callRpcService(User.create(serverUgi), User.create(ugi)));
  }

  private void setRpcProtection(String clientProtection, String serverProtection) {
    clientConf.set("hbase.rpc.protection", clientProtection);
    serverConf.set("hbase.rpc.protection", serverProtection);
  }

  /**
   * Test various combinations of Server and Client qops.
   */
  @Test
  public void testSaslWithCommonQop() throws Exception {
    setRpcProtection("privacy,authentication", "authentication");
    callRpcService();

    setRpcProtection("authentication", "privacy,authentication");
    callRpcService();

    setRpcProtection("integrity,authentication", "privacy,authentication");
    callRpcService();

    setRpcProtection("integrity,authentication", "integrity,authentication");
    callRpcService();

    setRpcProtection("privacy,authentication", "privacy,authentication");
    callRpcService();
  }

  @Test
  public void testSaslNoCommonQop() throws Exception {
    setRpcProtection("integrity", "privacy");
    SaslException se = assertThrows(SaslException.class, () -> callRpcService());
    assertEquals("No common protection layer between client and server", se.getMessage());
  }

  /**
   * Test sasl encryption with Crypto AES.
   */
  @Test
  public void testSaslWithCryptoAES() throws Exception {
    setRpcProtection("privacy", "privacy");
    setCryptoAES("true", "true");
    callRpcService();
  }

  /**
   * Test various combinations of Server and Client configuration for Crypto AES.
   */
  @Test
  public void testDifferentConfWithCryptoAES() throws Exception {
    setRpcProtection("privacy", "privacy");

    setCryptoAES("false", "true");
    callRpcService();

    setCryptoAES("true", "false");
    try {
      callRpcService();
      fail("The exception should be thrown out for the rpc timeout.");
    } catch (Exception e) {
      // ignore the expected exception
    }
  }

  private void setCryptoAES(String clientCryptoAES, String serverCryptoAES) {
    clientConf.set("hbase.rpc.crypto.encryption.aes.enabled", clientCryptoAES);
    serverConf.set("hbase.rpc.crypto.encryption.aes.enabled", serverCryptoAES);
  }

  /**
   * Sets up a RPC Server and a Client. Does a RPC checks the result. If an exception is thrown from
   * the stub, this function will throw root cause of that exception.
   */
  private void callRpcService(User serverUser, User clientUser) throws Exception {
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipals())
      .thenReturn(Collections.singletonList(HBaseKerberosUtils.KRB_PRINCIPAL));
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);

    InetSocketAddress isa = new InetSocketAddress(HOST, 0);

    RpcServer rpcServer = serverUser.getUGI()
      .doAs((PrivilegedExceptionAction<
        RpcServer>) () -> RpcServerFactory.createRpcServer(null, "AbstractTestSecureIPC",
          Lists.newArrayList(
            new RpcServer.BlockingServiceAndInterface((BlockingService) SERVICE, null)),
          isa, serverConf, new FifoRpcScheduler(serverConf, 1)));
    rpcServer.start();
    try (RpcClient rpcClient =
      RpcClientFactory.createClient(clientConf, HConstants.DEFAULT_CLUSTER_ID.toString())) {
      BlockingInterface stub =
        newBlockingStub(rpcClient, rpcServer.getListenerAddress(), clientUser);
      TestThread th1 = new TestThread(stub);
      final Throwable exception[] = new Throwable[1];
      Collections.synchronizedList(new ArrayList<Throwable>());
      Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override
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

  private void callRpcService(User clientUser) throws Exception {
    callRpcService(User.create(ugi), clientUser);
  }

  private void callRpcService() throws Exception {
    callRpcService(User.create(ugi));
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
          String result =
            stub.echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage(input).build())
              .getMessage();
          assertEquals(input, result);
        }
      } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

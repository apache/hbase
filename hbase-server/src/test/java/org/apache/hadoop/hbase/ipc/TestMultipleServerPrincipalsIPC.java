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
package org.apache.hadoop.hbase.ipc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.security.sasl.SaslException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.handler.codec.DecoderException;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;

/**
 * Tests for HBASE-28321, where we have multiple server principals candidates for a rpc service.
 * <p>
 * Put here just because we need to visit some package private classes under this package.
 */
@RunWith(Parameterized.class)
@Category({ SecurityTests.class, MediumTests.class })
public class TestMultipleServerPrincipalsIPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultipleServerPrincipalsIPC.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final File KEYTAB_FILE =
    new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String SERVER_PRINCIPAL;
  private static String SERVER_PRINCIPAL2;
  private static String CLIENT_PRINCIPAL;

  @Parameter(0)
  public Class<? extends RpcServer> rpcServerImpl;

  @Parameter(1)
  public Class<? extends RpcClient> rpcClientImpl;

  private Configuration clientConf;
  private Configuration serverConf;
  private UserGroupInformation clientUGI;
  private UserGroupInformation serverUGI;
  private RpcServer rpcServer;
  private RpcClient rpcClient;

  @Parameters(name = "{index}: rpcServerImpl={0}, rpcClientImpl={1}")
  public static List<Object[]> params() {
    List<Object[]> params = new ArrayList<>();
    List<Class<? extends RpcServer>> rpcServerImpls =
      Arrays.asList(NettyRpcServer.class, SimpleRpcServer.class);
    List<Class<? extends RpcClient>> rpcClientImpls =
      Arrays.asList(NettyRpcClient.class, BlockingRpcClient.class);
    for (Class<? extends RpcServer> rpcServerImpl : rpcServerImpls) {
      for (Class<? extends RpcClient> rpcClientImpl : rpcClientImpls) {
        params.add(new Object[] { rpcServerImpl, rpcClientImpl });
      }
    }
    return params;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    SERVER_PRINCIPAL = "server/" + HOST + "@" + KDC.getRealm();
    SERVER_PRINCIPAL2 = "server2/" + HOST + "@" + KDC.getRealm();
    CLIENT_PRINCIPAL = "client";
    KDC.createPrincipal(KEYTAB_FILE, CLIENT_PRINCIPAL, SERVER_PRINCIPAL, SERVER_PRINCIPAL2);
    setSecuredConfiguration(TEST_UTIL.getConfiguration());
    TEST_UTIL.getConfiguration().setInt("hbase.security.relogin.maxbackoff", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.security.relogin.maxretries", 0);
    TEST_UTIL.getConfiguration().setInt(RpcClient.FAILED_SERVER_EXPIRY_KEY, 10);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    if (KDC != null) {
      KDC.stop();
    }
  }

  private static void setSecuredConfiguration(Configuration conf) {
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
    conf.setBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, true);
  }

  private void loginAndStartRpcServer(String principal, int port) throws Exception {
    UserGroupInformation.setConfiguration(serverConf);
    serverUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal,
      KEYTAB_FILE.getCanonicalPath());
    rpcServer = serverUGI.doAs((PrivilegedExceptionAction<
      RpcServer>) () -> RpcServerFactory.createRpcServer(null, getClass().getSimpleName(),
        Lists.newArrayList(
          new RpcServer.BlockingServiceAndInterface(TestProtobufRpcServiceImpl.SERVICE, null)),
        new InetSocketAddress(HOST, port), serverConf, new FifoRpcScheduler(serverConf, 1)));
    rpcServer.start();
  }

  @Before
  public void setUp() throws Exception {
    clientConf = new Configuration(TEST_UTIL.getConfiguration());
    clientConf.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientImpl,
      RpcClient.class);
    String serverPrincipalConfigName = "hbase.test.multiple.principal.first";
    String serverPrincipalConfigName2 = "hbase.test.multiple.principal.second";
    clientConf.set(serverPrincipalConfigName, SERVER_PRINCIPAL);
    clientConf.set(serverPrincipalConfigName2, SERVER_PRINCIPAL2);
    serverConf = new Configuration(TEST_UTIL.getConfiguration());
    serverConf.setClass(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl,
      RpcServer.class);
    SecurityInfo securityInfo = new SecurityInfo(Kind.HBASE_AUTH_TOKEN, serverPrincipalConfigName2,
      serverPrincipalConfigName);
    SecurityInfo.addInfo(TestProtobufRpcProto.getDescriptor().getName(), securityInfo);

    UserGroupInformation.setConfiguration(clientConf);
    clientUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(CLIENT_PRINCIPAL,
      KEYTAB_FILE.getCanonicalPath());
    loginAndStartRpcServer(SERVER_PRINCIPAL, 0);
    rpcClient = clientUGI.doAs((PrivilegedExceptionAction<RpcClient>) () -> RpcClientFactory
      .createClient(clientConf, HConstants.DEFAULT_CLUSTER_ID.toString()));
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(rpcClient, true);
    rpcServer.stop();
  }

  private String echo(String msg) throws Exception {
    return clientUGI.doAs((PrivilegedExceptionAction<String>) () -> {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
        ServerName.valueOf(HOST, rpcServer.getListenerAddress().getPort(), -1), User.getCurrent(),
        10000);
      TestProtobufRpcProto.BlockingInterface stub = TestProtobufRpcProto.newBlockingStub(channel);
      return stub.echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage(msg).build())
        .getMessage();
    });
  }

  @Test
  public void testEcho() throws Exception {
    String msg = "Hello World";
    assertEquals(msg, echo(msg));
  }

  @Test
  public void testMaliciousServer() throws Exception {
    // reset the server principals so the principal returned by server does not match
    SecurityInfo securityInfo =
      SecurityInfo.getInfo(TestProtobufRpcProto.getDescriptor().getName());
    for (int i = 0; i < securityInfo.getServerPrincipals().size(); i++) {
      clientConf.set(securityInfo.getServerPrincipals().get(i),
        "valid_server_" + i + "/" + HOST + "@" + KDC.getRealm());
    }
    UndeclaredThrowableException error =
      assertThrows(UndeclaredThrowableException.class, () -> echo("whatever"));
    assertThat(error.getCause(), instanceOf(ServiceException.class));
    assertThat(error.getCause().getCause(), instanceOf(SaslException.class));
  }

  @Test
  public void testRememberLastSucceededServerPrincipal() throws Exception {
    // after this call we will remember the last succeeded server principal
    assertEquals("a", echo("a"));
    // shutdown the connection, but does not remove it from pool
    RpcConnection conn =
      Iterables.getOnlyElement(((AbstractRpcClient<?>) rpcClient).getConnections().values());
    conn.shutdown();
    // recreate rpc server with server principal2
    int port = rpcServer.getListenerAddress().getPort();
    rpcServer.stop();
    serverUGI.logoutUserFromKeytab();
    loginAndStartRpcServer(SERVER_PRINCIPAL2, port);
    // this time we will still use the remembered server principal, so we will get a sasl exception
    UndeclaredThrowableException error =
      assertThrows(UndeclaredThrowableException.class, () -> echo("a"));
    assertThat(error.getCause(), instanceOf(ServiceException.class));
    // created by IPCUtil.wrap, to prepend the server address
    assertThat(error.getCause().getCause(), instanceOf(IOException.class));
    // wraped IPCUtil.toIOE
    assertThat(error.getCause().getCause().getCause(), instanceOf(IOException.class));
    Throwable cause = error.getCause().getCause().getCause().getCause();
    // for netty rpc client, it is DecoderException, for blocking rpc client, it is already
    // RemoteExcetion
    assertThat(cause,
      either(instanceOf(DecoderException.class)).or(instanceOf(RemoteException.class)));
    RemoteException rme;
    if (!(cause instanceof RemoteException)) {
      assertThat(cause.getCause(), instanceOf(RemoteException.class));
      rme = (RemoteException) cause.getCause();
    } else {
      rme = (RemoteException) cause;
    }
    assertEquals(SaslException.class.getName(), rme.getClassName());
    // the above failure will clear the remembered server principal, so this time we will get the
    // correct one. We use retry here just because a failure of sasl negotiation will trigger a
    // relogin and it may take some time, and for netty based implementation the relogin is async
    TEST_UTIL.waitFor(10000, () -> {
      try {
        echo("a");
      } catch (UndeclaredThrowableException e) {
        Throwable t = e.getCause().getCause();
        assertThat(t, instanceOf(IOException.class));
        if (!(t instanceof FailedServerException)) {
          // for netty rpc client
          assertThat(e.getCause().getMessage(),
            containsString(RpcConnectionConstants.RELOGIN_IS_IN_PROGRESS));
        }
        return false;
      }
      return true;
    });
  }
}

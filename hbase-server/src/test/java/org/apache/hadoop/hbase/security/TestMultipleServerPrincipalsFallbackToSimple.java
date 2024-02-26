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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.FallbackDisallowedException;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;

/**
 * Test secure client connecting to a non secure server, where we have multiple server principal
 * candidates for a rpc service. See HBASE-28321.
 */
@RunWith(Parameterized.class)
@Category({ SecurityTests.class, MediumTests.class })
public class TestMultipleServerPrincipalsFallbackToSimple {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultipleServerPrincipalsFallbackToSimple.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final File KEYTAB_FILE =
    new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String SERVER_PRINCIPAL;
  private static String SERVER_PRINCIPAL2;
  private static String CLIENT_PRINCIPAL;

  @Parameter
  public Class<? extends RpcClient> rpcClientImpl;

  private Configuration clientConf;
  private UserGroupInformation clientUGI;
  private RpcServer rpcServer;
  private RpcClient rpcClient;

  @Parameters(name = "{index}: rpcClientImpl={0}")
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { NettyRpcClient.class },
      new Object[] { BlockingRpcClient.class });
  }

  private static void setSecuredConfiguration(Configuration conf) {
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
    conf.setBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, true);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    SERVER_PRINCIPAL = "server/" + HOST;
    SERVER_PRINCIPAL2 = "server2/" + HOST;
    CLIENT_PRINCIPAL = "client";
    KDC.createPrincipal(KEYTAB_FILE, CLIENT_PRINCIPAL, SERVER_PRINCIPAL, SERVER_PRINCIPAL2);
    TEST_UTIL.getConfiguration().setInt("hbase.security.relogin.maxbackoff", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.security.relogin.maxretries", 0);
    TEST_UTIL.getConfiguration().setInt(RpcClient.FAILED_SERVER_EXPIRY_KEY, 10);
  }

  @Before
  public void setUp() throws Exception {
    clientConf = new Configuration(TEST_UTIL.getConfiguration());
    setSecuredConfiguration(clientConf);
    clientConf.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientImpl,
      RpcClient.class);
    String serverPrincipalConfigName = "hbase.test.multiple.principal.first";
    String serverPrincipalConfigName2 = "hbase.test.multiple.principal.second";
    clientConf.set(serverPrincipalConfigName, "server/localhost@" + KDC.getRealm());
    clientConf.set(serverPrincipalConfigName2, "server2/localhost@" + KDC.getRealm());
    SecurityInfo securityInfo = new SecurityInfo(Kind.HBASE_AUTH_TOKEN, serverPrincipalConfigName2,
      serverPrincipalConfigName);
    SecurityInfo.addInfo(TestProtobufRpcProto.getDescriptor().getName(), securityInfo);

    UserGroupInformation.setConfiguration(clientConf);
    clientUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(CLIENT_PRINCIPAL,
      KEYTAB_FILE.getCanonicalPath());

    rpcServer = RpcServerFactory.createRpcServer(null, getClass().getSimpleName(),
      Lists.newArrayList(
        new RpcServer.BlockingServiceAndInterface(TestProtobufRpcServiceImpl.SERVICE, null)),
      new InetSocketAddress(HOST, 0), TEST_UTIL.getConfiguration(),
      new FifoRpcScheduler(TEST_UTIL.getConfiguration(), 1));
    rpcServer.start();
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(rpcClient, true);
    rpcServer.stop();
  }

  private RpcClient createClient() throws Exception {
    return clientUGI.doAs((PrivilegedExceptionAction<RpcClient>) () -> RpcClientFactory
      .createClient(clientConf, HConstants.DEFAULT_CLUSTER_ID.toString()));
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
  public void testAllowFallbackToSimple() throws Exception {
    clientConf.setBoolean(RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, true);
    rpcClient = createClient();
    assertEquals("allow", echo("allow"));
  }

  @Test
  public void testDisallowFallbackToSimple() throws Exception {
    clientConf.setBoolean(RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, false);
    rpcClient = createClient();
    UndeclaredThrowableException error =
      assertThrows(UndeclaredThrowableException.class, () -> echo("disallow"));
    Throwable cause = error.getCause().getCause().getCause();
    assertThat(cause, instanceOf(FallbackDisallowedException.class));
  }
}

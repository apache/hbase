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

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.setSecuredConfiguration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

@Category({ RPCTests.class, MediumTests.class })
public class TestRpcSkipInitialSaslHandshake {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcSkipInitialSaslHandshake.class);

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
    TEST_UTIL.getConfiguration().setInt(RpcClient.SOCKET_TIMEOUT_READ, 2000000000);
    TEST_UTIL.getConfiguration().setInt("hbase.security.relogin.maxretries", 1);
  }

  protected static void stopKDC() throws InterruptedException {
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
    clientConf.setBoolean(RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, true);
    serverConf = new Configuration(TEST_UTIL.getConfiguration());
  }

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
   * This test is for HBASE-27923,which NettyRpcServer may hange if it should skip initial sasl
   * handshake.
   */
  @Test
  public void test() throws Exception {
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipal())
      .thenReturn(HBaseKerberosUtils.KRB_PRINCIPAL);
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);

    final AtomicBoolean useSaslRef = new AtomicBoolean(false);
    NettyRpcServer rpcServer = new NettyRpcServer(null, getClass().getSimpleName(),
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress(HOST, 0), serverConf, new FifoRpcScheduler(serverConf, 1), true) {

      @Override
      protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler() {
        return new NettyRpcServerPreambleHandler(this) {
          private NettyServerRpcConnection conn;

          @Override
          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            super.channelRead0(ctx, msg);
            useSaslRef.set(conn.useSasl);

          }

          @Override
          protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
            conn = super.createNettyServerRpcConnection(channel);
            return conn;
          }
        };
      }
    };

    rpcServer.start();
    try (NettyRpcClient rpcClient =
      new NettyRpcClient(clientConf, HConstants.DEFAULT_CLUSTER_ID.toString(), null, null)) {
      BlockingInterface stub = newBlockingStub(rpcClient, rpcServer.getListenerAddress(),
        User.create(UserGroupInformation.getCurrentUser()));

      String response =
        stub.echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage("test").build())
          .getMessage();
      assertTrue("test".equals(response));
      assertFalse(useSaslRef.get());

    } finally {
      rpcServer.stop();
    }
  }
}

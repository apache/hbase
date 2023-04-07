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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContext;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContextProvider;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.AbstractTestIPC;
import org.apache.hadoop.hbase.ipc.FailingNettyRpcServer;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcClientConfigHelper;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.wal.NettyAsyncFSWALConfigHelper;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ RPCTests.class, MediumTests.class })
public class TestNettyTlsIPC extends AbstractTestIPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNettyTlsIPC.class);

  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility(CONF);

  private static X509TestContextProvider PROVIDER;

  private static NettyEventLoopGroupConfig EVENT_LOOP_GROUP_CONFIG;

  private static HRegionServer SERVER;

  @Parameterized.Parameter(0)
  public X509KeyType caKeyType;

  @Parameterized.Parameter(1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(2)
  public char[] keyPassword;

  @Parameterized.Parameter(3)
  public boolean acceptPlainText;

  @Parameterized.Parameter(4)
  public boolean clientTlsEnabled;

  private X509TestContext x509TestContext;

  @Parameterized.Parameters(
      name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, acceptPlainText={3},"
        + " clientTlsEnabled={4}")
  public static List<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (char[] keyPassword : new char[][] { "".toCharArray(), "pa$$w0rd".toCharArray() }) {
          // do not accept plain text
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, false, true });
          // support plain text and client enables tls
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, true, true });
          // support plain text and client disables tls
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, true, false });
        }
      }
    }
    return params;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    Security.addProvider(new BouncyCastleProvider());
    File dir = new File(UTIL.getDataTestDir(TestNettyTlsIPC.class.getSimpleName()).toString())
      .getCanonicalFile();
    FileUtils.forceMkdir(dir);
    // server must enable tls
    CONF.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_ENABLED, true);
    PROVIDER = new X509TestContextProvider(CONF, dir);
    EVENT_LOOP_GROUP_CONFIG =
      new NettyEventLoopGroupConfig(CONF, TestNettyTlsIPC.class.getSimpleName());
    NettyRpcClientConfigHelper.setEventLoopConfig(CONF, EVENT_LOOP_GROUP_CONFIG.group(),
      EVENT_LOOP_GROUP_CONFIG.clientChannelClass());
    NettyAsyncFSWALConfigHelper.setEventLoopConfig(CONF, EVENT_LOOP_GROUP_CONFIG.group(),
      EVENT_LOOP_GROUP_CONFIG.clientChannelClass());
    SERVER = mock(HRegionServer.class);
    when(SERVER.getEventLoopGroupConfig()).thenReturn(EVENT_LOOP_GROUP_CONFIG);
  }

  @AfterClass
  public static void tearDownAfterClass() throws InterruptedException {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    EVENT_LOOP_GROUP_CONFIG.group().shutdownGracefully().sync();
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    x509TestContext = PROVIDER.get(caKeyType, certKeyType, keyPassword);
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    CONF.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, acceptPlainText);
    CONF.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, clientTlsEnabled);
  }

  @After
  public void tearDown() {
    x509TestContext.clearConfigurations();
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }

  @Override
  protected RpcServer createRpcServer(String name, List<BlockingServiceAndInterface> services,
    InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler) throws IOException {
    return new NettyRpcServer(SERVER, name, services, bindAddress, conf, scheduler, true);
  }

  @Override
  protected AbstractRpcClient<?> createRpcClientNoCodec(Configuration conf) {
    return new NettyRpcClient(conf) {

      @Override
      protected Codec getCodec() {
        return null;
      }
    };
  }

  @Override
  protected AbstractRpcClient<?> createRpcClient(Configuration conf) {
    return new NettyRpcClient(conf);
  }

  @Override
  protected AbstractRpcClient<?> createRpcClientRTEDuringConnectionSetup(Configuration conf)
    throws IOException {
    return new NettyRpcClient(conf) {

      @Override
      protected boolean isTcpNoDelay() {
        throw new RuntimeException("Injected fault");
      }
    };
  }

  @Override
  protected RpcServer createTestFailingRpcServer(String name,
    List<BlockingServiceAndInterface> services, InetSocketAddress bindAddress, Configuration conf,
    RpcScheduler scheduler) throws IOException {
    return new FailingNettyRpcServer(SERVER, name, services, bindAddress, conf, scheduler);
  }
}

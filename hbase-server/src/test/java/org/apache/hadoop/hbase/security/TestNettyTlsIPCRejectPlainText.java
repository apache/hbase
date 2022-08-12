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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

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
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContext;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContextProvider;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

@RunWith(Parameterized.class)
@Category({ RPCTests.class, MediumTests.class })
public class TestNettyTlsIPCRejectPlainText {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNettyTlsIPCRejectPlainText.class);

  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  private static File DIR;

  private static X509TestContextProvider PROVIDER;

  @Parameterized.Parameter(0)
  public X509KeyType caKeyType;

  @Parameterized.Parameter(1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(2)
  public String keyPassword;

  private X509TestContext x509TestContext;

  private RpcServer rpcServer;

  private RpcClient rpcClient;

  @Parameterized.Parameters(name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}")
  public static List<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (String keyPassword : new String[] { "", "pa$$w0rd" }) {
          params.add(new Object[] { caKeyType, certKeyType, keyPassword });
        }
      }
    }
    return params;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    Security.addProvider(new BouncyCastleProvider());
    DIR = new File(UTIL.getDataTestDir(TestNettyTlsIPC.class.getSimpleName()).toString())
      .getCanonicalFile();
    FileUtils.forceMkdir(DIR);
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_ENABLED, true);
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, false);
    conf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, false);
    PROVIDER = new X509TestContextProvider(conf, DIR);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    x509TestContext = PROVIDER.get(caKeyType, certKeyType, keyPassword);
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    Configuration conf = UTIL.getConfiguration();
    rpcServer = new NettyRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), conf, new FifoRpcScheduler(conf, 1), true);
    rpcServer.start();
    rpcClient = new NettyRpcClient(conf);
  }

  @After
  public void tearDown() throws IOException {
    if (rpcServer != null) {
      rpcServer.stop();
    }
    Closeables.close(rpcClient, true);
    x509TestContext.clearConfigurations();
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }

  @Test
  public void testReject() throws IOException, ServiceException {
    BlockingInterface stub = newBlockingStub(rpcClient, rpcServer.getListenerAddress());
    ServiceException se = assertThrows(ServiceException.class,
      () -> stub.echo(null, EchoRequestProto.newBuilder().setMessage("hello world").build()));
    assertThat(se.getCause(), instanceOf(ConnectionClosedException.class));
  }
}

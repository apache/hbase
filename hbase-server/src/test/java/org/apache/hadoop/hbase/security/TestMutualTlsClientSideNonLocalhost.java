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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContext;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContextProvider;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

/**
 * Tests for client-side mTLS focusing on client hostname verification in the case when client and
 * server are on different hosts. We try to simulate this behaviour by querying the hostname with
 * <p>
 * InetAddress.getLocalHost()
 * </p>
 * Certificates are generated with the hostname in Subject Alternative Names, server binds
 * non-localhost interface and client connects via remote IP address. Parameter is set to verify
 * both TLS/plaintext and TLS-only cases.
 */
@Tag(RPCTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: supportPlaintext={0}")
public class TestMutualTlsClientSideNonLocalhost {

  private static HBaseCommonTestingUtility UTIL;

  private static File DIR;

  private static X509TestContextProvider PROVIDER;

  private X509TestContext x509TestContext;

  private RpcServer rpcServer;

  private RpcClient rpcClient;
  private TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub;

  private boolean supportPlaintext;

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  public TestMutualTlsClientSideNonLocalhost(boolean supportPlaintext) {
    this.supportPlaintext = supportPlaintext;
  }

  @BeforeAll
  public static void setUpBeforeClass() throws IOException {
    UTIL = new HBaseCommonTestingUtility();
    Security.addProvider(new BouncyCastleProvider());
    DIR =
      new File(UTIL.getDataTestDir(AbstractTestTlsRejectPlainText.class.getSimpleName()).toString())
        .getCanonicalFile();
    FileUtils.forceMkdir(DIR);
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, NettyRpcClient.class,
      RpcClient.class);
    conf.setClass(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, NettyRpcServer.class,
      RpcServer.class);
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_ENABLED, true);
    conf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, true);
    PROVIDER = new X509TestContextProvider(conf, DIR);
  }

  @AfterAll
  public static void cleanUp() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    UTIL.cleanupTestDir();
  }

  @BeforeEach
  public void setUp() throws Exception {
    x509TestContext = PROVIDER.get(X509KeyType.RSA, X509KeyType.RSA, "keyPassword".toCharArray());
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

    Configuration serverConf = new Configuration(UTIL.getConfiguration());
    Configuration clientConf = new Configuration(UTIL.getConfiguration());

    initialize(serverConf, clientConf);

    InetSocketAddress isa = new InetSocketAddress(InetAddress.getLocalHost(), 0);

    rpcServer = new NettyRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)), isa, serverConf,
      new FifoRpcScheduler(serverConf, 1), true);
    rpcServer.start();

    rpcClient = new NettyRpcClient(clientConf);
    stub = TestProtobufRpcServiceImpl.newBlockingStub(rpcClient, rpcServer.getListenerAddress());
  }

  private void initialize(Configuration serverConf, Configuration clientConf)
    throws GeneralSecurityException, IOException, OperatorCreationException {
    serverConf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, supportPlaintext);
    clientConf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_VERIFY_SERVER_HOSTNAME, true);
    x509TestContext.regenerateStores(X509KeyType.RSA, X509KeyType.RSA, KeyStoreFileType.JKS,
      KeyStoreFileType.JKS, InetAddress.getLocalHost().getHostName());
  }

  @AfterEach
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

  @TestTemplate
  public void testClientAuth() throws Exception {
    stub.echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage("hello world").build());
  }
}

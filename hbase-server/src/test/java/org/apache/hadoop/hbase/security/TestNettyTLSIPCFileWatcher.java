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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.io.FileChangeWatcher;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContext;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContextProvider;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

@Tag(RPCTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: keyType={0}, storeFileType={1}")
public class TestNettyTLSIPCFileWatcher {

  private static final Logger LOG = LoggerFactory.getLogger(TestNettyTLSIPCFileWatcher.class);

  private static final Configuration CONF = HBaseConfiguration.create();
  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility(CONF);
  private static HRegionServer SERVER;
  private static X509TestContextProvider PROVIDER;
  private static NettyEventLoopGroupConfig EVENT_LOOP_GROUP_CONFIG;

  private X509TestContext x509TestContext;

  private X509KeyType keyType;

  private KeyStoreFileType storeFileType;

  public static Stream<Arguments> parameters() {
    List<Arguments> params = new ArrayList<>();
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (KeyStoreFileType ks : KeyStoreFileType.values()) {
        params.add(Arguments.of(caKeyType, ks));
      }
    }
    return params.stream();
  }

  public TestNettyTLSIPCFileWatcher(X509KeyType keyType, KeyStoreFileType storeFileType) {
    this.keyType = keyType;
    this.storeFileType = storeFileType;
  }

  @BeforeAll
  public static void setUpBeforeClass() throws IOException {
    Security.addProvider(new BouncyCastleProvider());
    File dir =
      new File(UTIL.getDataTestDir(TestNettyTLSIPCFileWatcher.class.getSimpleName()).toString())
        .getCanonicalFile();
    FileUtils.forceMkdir(dir);
    // server must enable tls
    CONF.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_ENABLED, true);
    PROVIDER = new X509TestContextProvider(CONF, dir);
    EVENT_LOOP_GROUP_CONFIG =
      new NettyEventLoopGroupConfig(CONF, TestNettyTLSIPCFileWatcher.class.getSimpleName());
    SERVER = mock(HRegionServer.class);
    when(SERVER.getEventLoopGroupConfig()).thenReturn(EVENT_LOOP_GROUP_CONFIG);
  }

  @AfterAll
  public static void tearDownAfterClass() throws InterruptedException {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    EVENT_LOOP_GROUP_CONFIG.group().shutdownGracefully().sync();
    UTIL.cleanupTestDir();
  }

  @BeforeEach
  public void setUp() throws IOException {
    x509TestContext = PROVIDER.get(keyType, keyType, "keyPa$$word".toCharArray());
    x509TestContext.setConfigurations(storeFileType, storeFileType);
    CONF.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, false);
    CONF.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, true);
    CONF.setBoolean(X509Util.TLS_CERT_RELOAD, true);
    CONF.setLong(X509Util.HBASE_TLS_FILEPOLL_INTERVAL_MILLIS, 10);
  }

  @AfterEach
  public void tearDown() {
    x509TestContext.clearConfigurations();
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    x509TestContext.getConf().unset(X509Util.HBASE_TLS_FILEPOLL_INTERVAL_MILLIS);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }

  @TestTemplate
  public void testReplaceServerKeystore() throws IOException, ServiceException,
    GeneralSecurityException, OperatorCreationException, InterruptedException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));

    try {
      rpcServer.start();

      try (AbstractRpcClient<?> client = new NettyRpcClient(clientConf)) {
        TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
          newBlockingStub(client, rpcServer.getListenerAddress());
        HBaseRpcController pcrc = new HBaseRpcControllerImpl();
        String message = "hello";
        assertEquals(message,
          stub.echo(pcrc, TestProtos.EchoRequestProto.newBuilder().setMessage(message).build())
            .getMessage());
        assertNull(pcrc.cellScanner());
      }

      // truststore file change latch
      final CountDownLatch latch = new CountDownLatch(1);
      final Path trustStorePath = Paths.get(CONF.get(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION));
      createAndStartFileWatcher(trustStorePath, latch, Duration.ofMillis(20));

      Thread.sleep(1100L); // Ensure mtime changes on Java 8 (second granularity)

      // Replace keystore
      x509TestContext.regenerateStores(keyType, keyType, storeFileType, storeFileType);

      if (!latch.await(1, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out waiting for truststore file to be changed");
      }

      try (AbstractRpcClient<?> client = new NettyRpcClient(clientConf)) {
        TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
          newBlockingStub(client, rpcServer.getListenerAddress());
        HBaseRpcController pcrc = new HBaseRpcControllerImpl();
        String message = "hello";
        assertEquals(message,
          stub.echo(pcrc, TestProtos.EchoRequestProto.newBuilder().setMessage(message).build())
            .getMessage());
        assertNull(pcrc.cellScanner());
      }

    } finally {
      rpcServer.stop();
    }
  }

  @TestTemplate
  public void testReplaceClientAndServerKeystore() throws GeneralSecurityException, IOException,
    OperatorCreationException, ServiceException, InterruptedException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));

    try {
      rpcServer.start();

      try (AbstractRpcClient<?> client = new NettyRpcClient(clientConf)) {
        TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
          newBlockingStub(client, rpcServer.getListenerAddress());
        HBaseRpcController pcrc = new HBaseRpcControllerImpl();
        String message = "hello";
        assertEquals(message,
          stub.echo(pcrc, TestProtos.EchoRequestProto.newBuilder().setMessage(message).build())
            .getMessage());
        assertNull(pcrc.cellScanner());

        // truststore file change latch
        final CountDownLatch latch = new CountDownLatch(1);

        final Path trustStorePath = Paths.get(CONF.get(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION));
        createAndStartFileWatcher(trustStorePath, latch, Duration.ofMillis(20));

        Thread.sleep(1100L); // Ensure mtime changes on Java 8 (second granularity)

        // Replace keystore and cancel client connections
        x509TestContext.regenerateStores(keyType, keyType, storeFileType, storeFileType);
        client.cancelConnections(
          ServerName.valueOf(Address.fromSocketAddress(rpcServer.getListenerAddress()), 0L));

        if (!latch.await(1, TimeUnit.SECONDS)) {
          throw new AssertionError("Timed out waiting for truststore file to be changed");
        }

        assertEquals(message,
          stub.echo(pcrc, TestProtos.EchoRequestProto.newBuilder().setMessage(message).build())
            .getMessage());
        assertNull(pcrc.cellScanner());
      }
    } finally {
      rpcServer.stop();
    }
  }

  private RpcServer createRpcServer(String name,
    List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
    Configuration conf, RpcScheduler scheduler) throws IOException {
    return new NettyRpcServer(SERVER, name, services, bindAddress, conf, scheduler, true);
  }

  private void createAndStartFileWatcher(Path trustStorePath, CountDownLatch latch,
    Duration duration) throws IOException {
    FileChangeWatcher fileChangeWatcher = new FileChangeWatcher(trustStorePath,
      Objects.toString(trustStorePath.getFileName()), duration, watchEventFilePath -> {
        LOG.info("File " + watchEventFilePath.getFileName() + " has been changed.");
        latch.countDown();
      });
    fileChangeWatcher.start();
  }
}

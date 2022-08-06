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

import java.net.InetSocketAddress;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.io.crypto.tls.BaseX509ParameterizedTestCase;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

@RunWith(Parameterized.class)
@Category({ SecurityTests.class, MediumTests.class })
public class TestTlsIPC extends BaseX509ParameterizedTestCase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestTlsIPC.class);

  @Parameterized.Parameter()
  public X509KeyType caKeyType;

  @Parameterized.Parameter(value = 1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(value = 2)
  public String keyPassword;

  @Parameterized.Parameter(value = 3)
  public Integer paramIndex;

  @Parameterized.Parameters(
      name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, paramIndex={3}")
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    int paramIndex = 0;
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (String keyPassword : new String[] { KEY_EMPTY_PASSWORD, KEY_NON_EMPTY_PASSWORD }) {
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, paramIndex++ });
        }
      }
    }
    return params;
  }

  private static final String RPC_CLIENT_IMPL = NettyRpcClient.class.getName();
  private static final String RPC_SERVER_IMPL = NettyRpcServer.class.getName();
  private static final String HOST = "localhost";

  private UserGroupInformation ugi;
  private Configuration tlsConfiguration;
  private Configuration clientConf;
  private Configuration serverConf;

  @Override
  public void init(X509KeyType caKeyType, X509KeyType certKeyType, String keyPassword,
    Integer paramIndex) throws Exception {
    super.init(caKeyType, certKeyType, keyPassword, paramIndex);
    x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    tlsConfiguration = x509TestContext.getHbaseConf();
  }

  @Before
  public void setUpTest() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    String clientusername = "testuser";
    ugi =
      UserGroupInformation.createUserForTesting(clientusername, new String[] { clientusername });
    clientConf = HBaseConfiguration.create(tlsConfiguration);
    clientConf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, RPC_CLIENT_IMPL);
    serverConf = HBaseConfiguration.create(tlsConfiguration);
    serverConf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, RPC_SERVER_IMPL);
  }

  @After
  public void cleanUp() {
    x509TestContext.clearSystemProperties();
    x509TestContext.getHbaseConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getHbaseConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getHbaseConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }

  @Test
  public void testNoPlaintext() throws Exception {
    setTLSEncryption(true, false, true);
    callRpcService(User.create(ugi));
  }

  @Test
  public void testRejectPlaintext() {
    setTLSEncryption(true, false, false);
    Assert.assertThrows(ConnectionClosedException.class, () -> callRpcService(User.create(ugi)));
  }

  @Test
  public void testAcceptPlaintext() throws Exception {
    setTLSEncryption(true, true, false);
    callRpcService(User.create(ugi));
  }

  void setTLSEncryption(Boolean server, Boolean acceptPlaintext, Boolean client) {
    serverConf.set(HBASE_SERVER_NETTY_TLS_ENABLED, server.toString());
    serverConf.set(HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, acceptPlaintext.toString());
    clientConf.set(HBASE_CLIENT_NETTY_TLS_ENABLED, client.toString());
  }

  /**
   * Sets up a RPC Server and a Client. Does a RPC checks the result. If an exception is thrown from
   * the stub, this function will throw root cause of that exception.
   */
  private void callRpcService(User clientUser) throws Exception {
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
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

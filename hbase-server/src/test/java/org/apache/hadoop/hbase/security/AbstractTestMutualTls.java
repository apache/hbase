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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLHandshakeException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
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
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

public abstract class AbstractTestMutualTls {
  protected static HBaseCommonTestingUtil UTIL;

  protected static File DIR;

  protected static X509TestContextProvider PROVIDER;

  private X509TestContext x509TestContext;

  protected RpcServer rpcServer;

  protected RpcClient rpcClient;
  private TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub;

  @Parameterized.Parameter(0)
  public X509KeyType caKeyType;

  @Parameterized.Parameter(1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(2)
  public String keyPassword;
  @Parameterized.Parameter(3)
  public boolean expectSuccess;

  @Parameterized.Parameter(4)
  public boolean validateHostnames;

  @Parameterized.Parameter(5)
  public CertConfig certConfig;

  public enum CertConfig {
    // For no cert, we literally pass no certificate to the server. It's possible (assuming server
    // allows it based on ClientAuth mode) to use SSL without a KeyStore which will still do all
    // the handshaking but without a client cert. This is what we do here.
    // This mode only makes sense for client side, as server side must return a cert.
    NO_CLIENT_CERT,
    // For non-verifiable cert, we create a new certificate which is signed by a different
    // CA. So we're passing a cert, but the client/server can't verify it.
    NON_VERIFIABLE_CERT,
    // Good cert is the default mode, which uses a cert signed by the same CA both sides
    // and the hostname should match (localhost)
    GOOD_CERT,
    // For good cert/bad host, we create a new certificate signed by the same CA. But
    // this cert has a SANS that will not match the localhost peer.
    VERIFIABLE_CERT_WITH_BAD_HOST
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    UTIL = new HBaseCommonTestingUtil();
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
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, false);
    conf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, true);
    PROVIDER = new X509TestContextProvider(conf, DIR);
  }

  @AfterClass
  public static void cleanUp() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    UTIL.cleanupTestDir();
  }

  protected abstract void initialize(Configuration serverConf, Configuration clientConf)
    throws IOException, GeneralSecurityException, OperatorCreationException;

  @Before
  public void setUp() throws Exception {
    x509TestContext = PROVIDER.get(caKeyType, certKeyType, keyPassword.toCharArray());
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

    Configuration serverConf = new Configuration(UTIL.getConfiguration());
    Configuration clientConf = new Configuration(UTIL.getConfiguration());

    initialize(serverConf, clientConf);

    rpcServer = new NettyRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), serverConf, new FifoRpcScheduler(serverConf, 1), true);
    rpcServer.start();

    rpcClient = new NettyRpcClient(clientConf);
    stub = TestProtobufRpcServiceImpl.newBlockingStub(rpcClient, rpcServer.getListenerAddress());
  }

  protected void handleCertConfig(Configuration confToSet)
    throws GeneralSecurityException, IOException, OperatorCreationException {
    switch (certConfig) {
      case NO_CLIENT_CERT:
        // clearing out the keystore location will cause no cert to be sent.
        confToSet.set(X509Util.TLS_CONFIG_KEYSTORE_LOCATION, "");
        break;
      case NON_VERIFIABLE_CERT:
        // to simulate a bad cert, we inject a new keystore into the client side.
        // the same truststore exists, so it will still successfully verify the server cert
        // but since the new client keystore cert is created from a new CA (which the server doesn't
        // have),
        // the server will not be able to verify it.
        X509TestContext context =
          PROVIDER.get(caKeyType, certKeyType, "random value".toCharArray());
        context.setKeystoreConfigurations(KeyStoreFileType.JKS, confToSet);
        break;
      case VERIFIABLE_CERT_WITH_BAD_HOST:
        // to simulate a good cert with a bad host, we need to create a new cert using the existing
        // context's CA/truststore. Here we can pass any random SANS, as long as it won't match
        // localhost or any reasonable name that this test might run on.
        X509Certificate cert = x509TestContext.newCert(new X500NameBuilder(BCStyle.INSTANCE)
          .addRDN(BCStyle.CN,
            MethodHandles.lookup().lookupClass().getCanonicalName() + " With Bad Host Test")
          .build(), "www.example.com");
        x509TestContext.cloneWithNewKeystoreCert(cert)
          .setKeystoreConfigurations(KeyStoreFileType.JKS, confToSet);
        break;
      default:
        break;
    }
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
  public void testClientAuth() throws Exception {
    if (expectSuccess) {
      // we expect no exception, so if one is thrown the test will fail
      submitRequest();
    } else {
      ServiceException se = assertThrows(ServiceException.class, this::submitRequest);
      // The SSLHandshakeException is encapsulated differently depending on the TLS version
      boolean seenSSLHandshakeException = false;
      Throwable current = se;
      do {
        if (current instanceof SSLHandshakeException) {
          seenSSLHandshakeException = true;
          break;
        }
        current = current.getCause();
      } while (current != null);
      assertTrue("Exception chain does not include SSLHandshakeException",
        seenSSLHandshakeException);
    }
  }

  private void submitRequest() throws ServiceException {
    stub.echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage("hello world").build());
  }
}

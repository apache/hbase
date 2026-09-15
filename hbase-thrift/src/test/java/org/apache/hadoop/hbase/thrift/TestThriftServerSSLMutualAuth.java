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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.TestThriftServerCmdLine.createBoundServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.hbase.thirdparty.org.apache.thrift.protocol.TProtocol;
import org.apache.hbase.thirdparty.org.apache.thrift.transport.TMemoryBuffer;

/**
 * Exercises the role-scoped {@code hbase.thrift.ssl.server.*} configuration and the new
 * mutual-TLS activation on the Thrift-over-HTTP transport. Complements
 * {@link TestThriftHttpServerSSL}, which covers plain (server-only) TLS termination.
 */
@Tag(ClientTests.TAG)
@Tag(LargeTests.TAG)
public class TestThriftServerSSLMutualAuth {

  private static final Logger LOG = LoggerFactory.getLogger(TestThriftServerSSLMutualAuth.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String KEY_STORE_PASSWORD = "myKSPassword";
  private static final String TRUST_STORE_PASSWORD = "myTSPassword";
  private static final String CLIENT_KEY_STORE_PASSWORD = "myClientKSPassword";

  private File keyDir;
  private ThriftServerRunner tsr;
  private HttpPost httpPost;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(Constants.USE_HTTP_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, false);
    TEST_UTIL.startMiniCluster();
    EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManager.reset();
  }

  @BeforeEach
  public void setUp() throws Exception {
    initializeAlgorithmId();
    keyDir = initKeystoreDir();
    keyDir.deleteOnExit();

    // Server identity + a truststore holding the server's cert (used by the test client to
    // trust the server).
    KeyPair serverKeyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate serverCertificate = KeyStoreTestUtil.generateCertificate(
      "CN=localhost, O=server", serverKeyPair, 30, "SHA1withRSA");
    generateTrustStore(getServerTruststoreFilePath(), serverCertificate);
    generateKeyStore(getServerKeystoreFilePath(), serverKeyPair, serverCertificate);

    // Distinct client cert (single-EKU clientAuth in spirit) and a truststore holding that cert
    // — this is what the server uses to validate presented client certificates.
    KeyPair clientKeyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate clientCertificate = KeyStoreTestUtil.generateCertificate("CN=client, O=client",
      clientKeyPair, 30, "SHA1withRSA");
    generateTrustStore(getClientCaTruststoreFilePath(), clientCertificate);
    generateKeyStoreWithPassword(getClientKeystoreFilePath(), clientKeyPair, clientCertificate,
      CLIENT_KEY_STORE_PASSWORD);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (httpPost != null) {
      httpPost.releaseConnection();
    }
    if (tsr != null) {
      tsr.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Role-scoped keystore configuration.
  // ---------------------------------------------------------------------------

  /**
   * With only the role-scoped {@code hbase.thrift.ssl.server.keystore.*} keys set (legacy keys
   * unset), a plain HTTPS request succeeds — proving the server-scoped keys are the ones the
   * bootstrap actually reads.
   */
  @Test
  public void testServerUsesRoleScopedKeystoreWhenSet() throws Exception {
    Configuration conf = baseConf();
    conf.set(Constants.THRIFT_SSL_SERVER_KEYSTORE_STORE_KEY, getServerKeystoreFilePath());
    conf.set(Constants.THRIFT_SSL_SERVER_KEYSTORE_PASSWORD_KEY, KEY_STORE_PASSWORD);
    conf.set(Constants.THRIFT_SSL_SERVER_KEYSTORE_KEYPASSWORD_KEY, KEY_STORE_PASSWORD);

    startServer(conf);
    doRequestExpectingSuccess(clientBuilderWithServerTrust());
  }

  /**
   * Backward-compatibility regression: existing deployments that know only about the legacy
   * unscoped keys must continue to work.
   */
  @Test
  public void testServerFallsBackToLegacyKeystore() throws Exception {
    Configuration conf = baseConf();
    conf.set(Constants.THRIFT_SSL_KEYSTORE_STORE_KEY, getServerKeystoreFilePath());
    conf.set(Constants.THRIFT_SSL_KEYSTORE_PASSWORD_KEY, KEY_STORE_PASSWORD);
    conf.set(Constants.THRIFT_SSL_KEYSTORE_KEYPASSWORD_KEY, KEY_STORE_PASSWORD);

    startServer(conf);
    doRequestExpectingSuccess(clientBuilderWithServerTrust());
  }

  // ---------------------------------------------------------------------------
  // Mutual TLS (client auth mode).
  // ---------------------------------------------------------------------------

  /**
   * With {@code client.auth.mode=NONE} (default), a client presenting no certificate is
   * accepted — matches today's behavior.
   */
  @Test
  public void testClientAuthNoneAcceptsClientWithoutCert() throws Exception {
    Configuration conf = baseConfWithLegacyKeystore();
    conf.set(Constants.THRIFT_SSL_CLIENT_AUTH_MODE_KEY, "NONE");

    startServer(conf);
    doRequestExpectingSuccess(clientBuilderWithServerTrust());
  }

  /** {@code WANT} lets anonymous clients through. */
  @Test
  public void testClientAuthWantAllowsAnonymousClient() throws Exception {
    Configuration conf = baseConfWithLegacyKeystore();
    // WANT needs the server to know which CAs it would trust if a client did present a cert.
    conf.set(Constants.THRIFT_SSL_SERVER_TRUSTSTORE_STORE_KEY, getClientCaTruststoreFilePath());
    conf.set(Constants.THRIFT_SSL_SERVER_TRUSTSTORE_PASSWORD_KEY, TRUST_STORE_PASSWORD);
    conf.set(Constants.THRIFT_SSL_CLIENT_AUTH_MODE_KEY, "WANT");

    startServer(conf);
    doRequestExpectingSuccess(clientBuilderWithServerTrust());
  }

  /**
   * {@code NEED} rejects clients that do not present a valid client certificate. The handshake
   * fails before any HTTP response is produced.
   */
  @Test
  public void testClientAuthNeedRejectsClientWithoutCert() throws Exception {
    Configuration conf = baseConfWithLegacyKeystore();
    conf.set(Constants.THRIFT_SSL_SERVER_TRUSTSTORE_STORE_KEY, getClientCaTruststoreFilePath());
    conf.set(Constants.THRIFT_SSL_SERVER_TRUSTSTORE_PASSWORD_KEY, TRUST_STORE_PASSWORD);
    conf.set(Constants.THRIFT_SSL_CLIENT_AUTH_MODE_KEY, "NEED");

    startServer(conf);
    // The Apache HttpClient surface may raise either SSLHandshakeException directly or wrap it
    // in an IOException — both are acceptable evidence that the handshake was refused.
    assertThrows(IOException.class, () -> doRequest(clientBuilderWithServerTrust()));
  }

  /** {@code NEED} accepts a client that presents a certificate trusted by the server. */
  @Test
  public void testClientAuthNeedAcceptsClientWithValidCert() throws Exception {
    Configuration conf = baseConfWithLegacyKeystore();
    conf.set(Constants.THRIFT_SSL_SERVER_TRUSTSTORE_STORE_KEY, getClientCaTruststoreFilePath());
    conf.set(Constants.THRIFT_SSL_SERVER_TRUSTSTORE_PASSWORD_KEY, TRUST_STORE_PASSWORD);
    conf.set(Constants.THRIFT_SSL_CLIENT_AUTH_MODE_KEY, "NEED");

    startServer(conf);
    doRequestExpectingSuccess(clientBuilderWithMutualTrust());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private Configuration baseConf() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(Constants.THRIFT_SSL_ENABLED_KEY, true);
    return conf;
  }

  private Configuration baseConfWithLegacyKeystore() throws Exception {
    Configuration conf = baseConf();
    conf.set(Constants.THRIFT_SSL_KEYSTORE_STORE_KEY, getServerKeystoreFilePath());
    conf.set(Constants.THRIFT_SSL_KEYSTORE_PASSWORD_KEY, KEY_STORE_PASSWORD);
    conf.set(Constants.THRIFT_SSL_KEYSTORE_KEYPASSWORD_KEY, KEY_STORE_PASSWORD);
    return conf;
  }

  private void startServer(Configuration conf) throws Exception {
    tsr = createBoundServer(() -> new ThriftServer(conf));
    String url = "https://" + HConstants.LOCALHOST + ":" + tsr.getThriftServer().listenPort;
    httpPost = new HttpPost(url);
    httpPost.setHeader("Content-Type", "application/x-thrift");
    httpPost.setHeader("Accept", "application/x-thrift");
    httpPost.setHeader("User-Agent", "Java/THttpClient/HC");
  }

  private HttpClientBuilder clientBuilderWithServerTrust() throws Exception {
    KeyStore trustStore = loadJksTrustStore(getServerTruststoreFilePath(), TRUST_STORE_PASSWORD);
    SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(trustStore, null).build();
    return HttpClients.custom().setSSLContext(sslContext);
  }

  /**
   * Builds a client that (a) trusts the server's cert and (b) presents the client keystore. Used
   * to satisfy {@code client.auth.mode=NEED}.
   */
  private HttpClientBuilder clientBuilderWithMutualTrust() throws Exception {
    KeyStore trustStore = loadJksTrustStore(getServerTruststoreFilePath(), TRUST_STORE_PASSWORD);
    KeyStore clientKs;
    try (InputStream in = new BufferedInputStream(
      Files.newInputStream(new File(getClientKeystoreFilePath()).toPath()))) {
      clientKs = KeyStore.getInstance("JKS");
      clientKs.load(in, CLIENT_KEY_STORE_PASSWORD.toCharArray());
    }
    SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(trustStore, null)
      .loadKeyMaterial(clientKs, CLIENT_KEY_STORE_PASSWORD.toCharArray()).build();
    return HttpClients.custom().setSSLContext(sslContext);
  }

  private void doRequestExpectingSuccess(HttpClientBuilder builder) throws Exception {
    try (CloseableHttpClient httpClient = builder.build()) {
      CloseableHttpResponse response = doOneRoundTrip(httpClient);
      assertEquals(HttpURLConnection.HTTP_OK, response.getStatusLine().getStatusCode());
    }
  }

  private void doRequest(HttpClientBuilder builder) throws Exception {
    try (CloseableHttpClient httpClient = builder.build()) {
      doOneRoundTrip(httpClient);
    }
  }

  private CloseableHttpResponse doOneRoundTrip(CloseableHttpClient httpClient) throws Exception {
    TMemoryBuffer memoryBuffer = new TMemoryBuffer(100);
    TProtocol prot = new TBinaryProtocol(memoryBuffer);
    Hbase.Client client = new Hbase.Client(prot);
    client.send_getClusterId();
    httpPost.setEntity(new ByteArrayEntity(memoryBuffer.getArray()));
    return httpClient.execute(httpPost);
  }

  private static KeyStore loadJksTrustStore(String path, String password) throws Exception {
    try (InputStream in = new BufferedInputStream(Files.newInputStream(new File(path).toPath()))) {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(in, password.toCharArray());
      return ks;
    }
  }

  // Workaround for jdk8 292 bug. See https://github.com/bcgit/bc-java/issues/941
  // Below is a workaround described in above URL. Issue fingered first in comments in
  // HBASE-25920 Support Hadoop 3.3.1
  private static void initializeAlgorithmId() {
    try {
      Class<?> algoId = Class.forName("sun.security.x509.AlgorithmId");
      Method method = algoId.getMethod("get", String.class);
      method.setAccessible(true);
      method.invoke(null, "PBEWithSHA1AndDESede");
    } catch (Exception e) {
      LOG.warn("failed to initialize AlgorithmId", e);
    }
  }

  private File initKeystoreDir() {
    String dataTestDir = TEST_UTIL.getDataTestDir().toString();
    File keystoreDir =
      new File(dataTestDir, TestThriftServerSSLMutualAuth.class.getSimpleName() + "_keys");
    keystoreDir.mkdirs();
    return keystoreDir;
  }

  private static void generateKeyStore(String keyStorePath, KeyPair keyPair, X509Certificate cert)
    throws Exception {
    KeyStoreTestUtil.createKeyStore(keyStorePath, KEY_STORE_PASSWORD, KEY_STORE_PASSWORD,
      "serverKS", keyPair.getPrivate(), cert);
  }

  private static void generateKeyStoreWithPassword(String keyStorePath, KeyPair keyPair,
    X509Certificate cert, String password) throws Exception {
    KeyStoreTestUtil.createKeyStore(keyStorePath, password, password, "clientKS",
      keyPair.getPrivate(), cert);
  }

  private static void generateTrustStore(String path, X509Certificate cert) throws Exception {
    KeyStoreTestUtil.createTrustStore(path, TRUST_STORE_PASSWORD, "ts", cert);
  }

  private String getServerKeystoreFilePath() {
    return String.format("%s/serverKS.jks", keyDir.getAbsolutePath());
  }

  private String getServerTruststoreFilePath() {
    return String.format("%s/serverTS.jks", keyDir.getAbsolutePath());
  }

  private String getClientKeystoreFilePath() {
    return String.format("%s/clientKS.jks", keyDir.getAbsolutePath());
  }

  private String getClientCaTruststoreFilePath() {
    return String.format("%s/clientCA.jks", keyDir.getAbsolutePath());
  }

}
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
import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.hbase.HBaseClassTestRule;
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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ClientTests.class, LargeTests.class })
public class TestThriftHttpServerSSL {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestThriftHttpServerSSL.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestThriftHttpServerSSL.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String KEY_STORE_PASSWORD = "myKSPassword";
  private static final String TRUST_STORE_PASSWORD = "myTSPassword";

  private File keyDir;
  private HttpClientBuilder httpClientBuilder;
  private ThriftServerRunner tsr;
  private HttpPost httpPost = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(Constants.USE_HTTP_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, false);
    TEST_UTIL.startMiniCluster();
    // ensure that server time increments every time we do an operation, otherwise
    // successive puts having the same timestamp will override each other
    EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManager.reset();
  }

  @Before
  public void setUp() throws Exception {
    initializeAlgorithmId();
    keyDir = initKeystoreDir();
    keyDir.deleteOnExit();
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");

    X509Certificate serverCertificate =
      KeyStoreTestUtil.generateCertificate("CN=localhost, O=server", keyPair, 30, "SHA1withRSA");

    generateTrustStore(serverCertificate);
    generateKeyStore(keyPair, serverCertificate);

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(Constants.THRIFT_SSL_ENABLED_KEY, true);
    conf.set(Constants.THRIFT_SSL_KEYSTORE_STORE_KEY, getKeystoreFilePath());
    conf.set(Constants.THRIFT_SSL_KEYSTORE_PASSWORD_KEY, KEY_STORE_PASSWORD);
    conf.set(Constants.THRIFT_SSL_KEYSTORE_KEYPASSWORD_KEY, KEY_STORE_PASSWORD);

    tsr = createBoundServer(() -> new ThriftServer(conf));
    String url = "https://" + HConstants.LOCALHOST + ":" + tsr.getThriftServer().listenPort;

    KeyStore trustStore;
    trustStore = KeyStore.getInstance("JKS");
    try (InputStream inputStream =
      new BufferedInputStream(Files.newInputStream(new File(getTruststoreFilePath()).toPath()))) {
      trustStore.load(inputStream, TRUST_STORE_PASSWORD.toCharArray());
    }

    httpClientBuilder = HttpClients.custom();
    SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(trustStore, null).build();
    httpClientBuilder.setSSLContext(sslcontext);

    httpPost = new HttpPost(url);
    httpPost.setHeader("Content-Type", "application/x-thrift");
    httpPost.setHeader("Accept", "application/x-thrift");
    httpPost.setHeader("User-Agent", "Java/THttpClient/HC");
  }

  @After
  public void tearDown() throws IOException {
    if (httpPost != null) {
      httpPost.releaseConnection();
    }
    if (tsr != null) {
      tsr.close();
    }
  }

  @Test
  public void testSecurityHeaders() throws Exception {
    try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
      TMemoryBuffer memoryBuffer = new TMemoryBuffer(100);
      TProtocol prot = new TBinaryProtocol(memoryBuffer);
      Hbase.Client client = new Hbase.Client(prot);
      client.send_getClusterId();

      httpPost.setEntity(new ByteArrayEntity(memoryBuffer.getArray()));
      CloseableHttpResponse httpResponse = httpClient.execute(httpPost);

      assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getStatusLine().getStatusCode());
      assertEquals("DENY", httpResponse.getFirstHeader("X-Frame-Options").getValue());

      assertEquals("nosniff", httpResponse.getFirstHeader("X-Content-Type-Options").getValue());
      assertEquals("1; mode=block", httpResponse.getFirstHeader("X-XSS-Protection").getValue());

      assertEquals("default-src https: data: 'unsafe-inline' 'unsafe-eval'",
        httpResponse.getFirstHeader("Content-Security-Policy").getValue());
      assertEquals("max-age=63072000;includeSubDomains;preload",
        httpResponse.getFirstHeader("Strict-Transport-Security").getValue());
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
    File keystoreDir = new File(dataTestDir, TestThriftHttpServer.class.getSimpleName() + "_keys");
    keystoreDir.mkdirs();
    return keystoreDir;
  }

  private void generateKeyStore(KeyPair keyPair, X509Certificate serverCertificate)
    throws Exception {
    String keyStorePath = getKeystoreFilePath();
    KeyStoreTestUtil.createKeyStore(keyStorePath, KEY_STORE_PASSWORD, KEY_STORE_PASSWORD,
      "serverKS", keyPair.getPrivate(), serverCertificate);
  }

  private void generateTrustStore(X509Certificate serverCertificate) throws Exception {
    String trustStorePath = getTruststoreFilePath();
    KeyStoreTestUtil.createTrustStore(trustStorePath, TRUST_STORE_PASSWORD, "serverTS",
      serverCertificate);
  }

  private String getKeystoreFilePath() {
    return String.format("%s/serverKS.%s", keyDir.getAbsolutePath(), "jks");
  }

  private String getTruststoreFilePath() {
    return String.format("%s/serverTS.%s", keyDir.getAbsolutePath(), "jks");
  }
}

/**
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
package org.apache.hadoop.hbase.rest;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.lang.reflect.Method;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RestTests.class, MediumTests.class})
public class TestRESTServerSSL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRESTServerSSL.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRESTServerSSL.class);

  private static final String KEY_STORE_PASSWORD = "myKSPassword";
  private static final String TRUST_STORE_PASSWORD = "myTSPassword";

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = new HBaseRESTTestingUtility();
  private static Client sslClient;
  private static File keyDir;
  private Configuration conf;

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

  @BeforeClass
  public static void beforeClass() throws Exception {
    initializeAlgorithmId();
    keyDir = initKeystoreDir();
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate serverCertificate = KeyStoreTestUtil.generateCertificate(
      "CN=localhost, O=server", keyPair, 30, "SHA1withRSA");

    generateTrustStore("jks", serverCertificate);
    generateTrustStore("jceks", serverCertificate);
    generateTrustStore("pkcs12", serverCertificate);

    generateKeyStore("jks", keyPair, serverCertificate);
    generateKeyStore("jceks", keyPair, serverCertificate);
    generateKeyStore("pkcs12", keyPair, serverCertificate);

    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // this will also delete the generated test keystore / teststore files,
    // as we were placing them under the dataTestDir used by the minicluster
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeEachTest() {
    conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(Constants.REST_SSL_ENABLED, "true");
    conf.set(Constants.REST_SSL_KEYSTORE_KEYPASSWORD, KEY_STORE_PASSWORD);
    conf.set(Constants.REST_SSL_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD);
    conf.set(Constants.REST_SSL_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD);
  }

  @After
  public void tearDownAfterTest() {
    REST_TEST_UTIL.shutdownServletContainer();
  }

  @Test
  public void testSslConnection() throws Exception {
    startRESTServerWithDefaultKeystoreType();

    Response response = sslClient.get("/version", Constants.MIMETYPE_TEXT);
    assertEquals(200, response.getCode());

    // Default security headers
    assertEquals("max-age=63072000;includeSubDomains;preload",
      response.getHeader("Strict-Transport-Security"));
    assertEquals("default-src https: data: 'unsafe-inline' 'unsafe-eval'",
      response.getHeader("Content-Security-Policy"));
  }

  @Test(expected = org.apache.http.client.ClientProtocolException.class)
  public void testNonSslClientDenied() throws Exception {
    startRESTServerWithDefaultKeystoreType();

    Cluster localCluster = new Cluster().add("localhost", REST_TEST_UTIL.getServletPort());
    Client nonSslClient = new Client(localCluster, false);

    nonSslClient.get("/version");
  }

  @Test
  public void testSslConnectionUsingKeystoreFormatJKS() throws Exception {
    startRESTServer("jks");

    Response response = sslClient.get("/version", Constants.MIMETYPE_TEXT);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testSslConnectionUsingKeystoreFormatJCEKS() throws Exception {
    startRESTServer("jceks");

    Response response = sslClient.get("/version", Constants.MIMETYPE_TEXT);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testSslConnectionUsingKeystoreFormatPKCS12() throws Exception {
    startRESTServer("pkcs12");

    Response response = sslClient.get("/version", Constants.MIMETYPE_TEXT);
    assertEquals(200, response.getCode());
  }



  private static File initKeystoreDir() {
    String dataTestDir = TEST_UTIL.getDataTestDir().toString();
    File keystoreDir = new File(dataTestDir, TestRESTServerSSL.class.getSimpleName() + "_keys");
    keystoreDir.mkdirs();
    return keystoreDir;
  }

  private static void generateKeyStore(String keyStoreType, KeyPair keyPair,
    X509Certificate serverCertificate) throws Exception {
    String keyStorePath = getKeystoreFilePath(keyStoreType);
    KeyStoreTestUtil.createKeyStore(keyStorePath, KEY_STORE_PASSWORD, KEY_STORE_PASSWORD,
      "serverKS", keyPair.getPrivate(), serverCertificate, keyStoreType);
  }

  private static void generateTrustStore(String trustStoreType, X509Certificate serverCertificate)
    throws Exception {
    String trustStorePath = getTruststoreFilePath(trustStoreType);
    KeyStoreTestUtil.createTrustStore(trustStorePath, TRUST_STORE_PASSWORD, "serverTS",
      serverCertificate, trustStoreType);
  }

  private static String getKeystoreFilePath(String keyStoreType) {
    return String.format("%s/serverKS.%s", keyDir.getAbsolutePath(), keyStoreType);
  }

  private static String getTruststoreFilePath(String trustStoreType) {
    return String.format("%s/serverTS.%s", keyDir.getAbsolutePath(), trustStoreType);
  }

  private void startRESTServerWithDefaultKeystoreType() throws Exception {
    conf.set(Constants.REST_SSL_KEYSTORE_STORE, getKeystoreFilePath("jks"));
    conf.set(Constants.REST_SSL_TRUSTSTORE_STORE, getTruststoreFilePath("jks"));

    REST_TEST_UTIL.startServletContainer(conf);
    Cluster localCluster = new Cluster().add("localhost", REST_TEST_UTIL.getServletPort());
    sslClient = new Client(localCluster, getTruststoreFilePath("jks"),
      Optional.of(TRUST_STORE_PASSWORD), Optional.empty());
  }

  private void startRESTServer(String storeType) throws Exception {
    conf.set(Constants.REST_SSL_KEYSTORE_TYPE, storeType);
    conf.set(Constants.REST_SSL_KEYSTORE_STORE, getKeystoreFilePath(storeType));

    conf.set(Constants.REST_SSL_TRUSTSTORE_STORE, getTruststoreFilePath(storeType));
    conf.set(Constants.REST_SSL_TRUSTSTORE_TYPE, storeType);

    REST_TEST_UTIL.startServletContainer(conf);
    Cluster localCluster = new Cluster().add("localhost", REST_TEST_UTIL.getServletPort());
    sslClient = new Client(localCluster, getTruststoreFilePath(storeType),
                           Optional.of(TRUST_STORE_PASSWORD), Optional.of(storeType));
  }

}

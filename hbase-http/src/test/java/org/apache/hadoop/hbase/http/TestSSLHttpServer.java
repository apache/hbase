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
package org.apache.hadoop.hbase.http;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.GeneralSecurityException;
import javax.net.ssl.HttpsURLConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This testcase issues SSL certificates configures the HttpServer to serve
 * HTTPS using the created certficates and calls an echo servlet using the
 * corresponding HTTPS URL.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestSSLHttpServer extends HttpServerFunctionalTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSSLHttpServer.class);

  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" + TestSSLHttpServer.class.getSimpleName();

  private static final Logger LOG = LoggerFactory.getLogger(TestSSLHttpServer.class);
  private static Configuration serverConf;
  private static HttpServer server;
  private static URL baseUrl;
  private static File keystoresDir;
  private static String sslConfDir;
  private static SSLFactory clientSslFactory;
  private static HBaseCommonTestingUtil HTU;

  @BeforeClass
  public static void setup() throws Exception {

    HTU = new HBaseCommonTestingUtil();
    serverConf = HTU.getConfiguration();

    serverConf.setInt(HttpServer.HTTP_MAX_THREADS, TestHttpServer.MAX_THREADS);
    serverConf.setBoolean(ServerConfigurationKeys.HBASE_SSL_ENABLED_KEY, true);

    keystoresDir = new File(HTU.getDataTestDir("keystore").toString());
    keystoresDir.mkdirs();

    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir.getAbsolutePath(), sslConfDir, serverConf, false);
    Configuration clientConf = new Configuration(false);
    clientConf.addResource(serverConf.get(SSLFactory.SSL_CLIENT_CONF_KEY));
    serverConf.addResource(serverConf.get(SSLFactory.SSL_SERVER_CONF_KEY));
    clientConf.set(SSLFactory.SSL_CLIENT_CONF_KEY, serverConf.get(SSLFactory.SSL_CLIENT_CONF_KEY));
    
    clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, clientConf);
    clientSslFactory.init();

    server = new HttpServer.Builder()
      .setName("test")
      .addEndpoint(new URI("https://localhost"))
      .setConf(serverConf)
      .keyPassword(HBaseConfiguration.getPassword(serverConf, "ssl.server.keystore.keypassword",
        null))
      .keyStore(serverConf.get("ssl.server.keystore.location"),
        HBaseConfiguration.getPassword(serverConf, "ssl.server.keystore.password", null),
        clientConf.get("ssl.server.keystore.type", "jks"))
      .trustStore(serverConf.get("ssl.server.truststore.location"),
        HBaseConfiguration.getPassword(serverConf, "ssl.server.truststore.password", null),
        serverConf.get("ssl.server.truststore.type", "jks")).build();
    server.addUnprivilegedServlet("echo", "/echo", TestHttpServer.EchoServlet.class);
    server.start();
    baseUrl = new URL("https://"
      + NetUtils.getHostPortString(server.getConnectorAddress(0)));
    LOG.info("HTTP server started: " + baseUrl);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    server.stop();
    FileUtil.fullyDelete(new File(HTU.getDataTestDir().toString()));
    KeyStoreTestUtil.cleanupSSLConfig(serverConf);
    clientSslFactory.destroy();
  }

  @Test
  public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", readOut(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n", readOut(new URL(baseUrl,
        "/echo?a=b&c<=d&e=>")));
  }

  @Test
  public void testSecurityHeaders() throws IOException, GeneralSecurityException {
    HttpsURLConnection conn = (HttpsURLConnection) baseUrl.openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
    assertEquals(HttpsURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals("max-age=63072000;includeSubDomains;preload",
      conn.getHeaderField("Strict-Transport-Security"));
    assertEquals("default-src https: data: 'unsafe-inline' 'unsafe-eval'",
      conn.getHeaderField("Content-Security-Policy"));
  }

  private static String readOut(URL url) throws Exception {
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
    InputStream in = conn.getInputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(in, out, 1024);
    return out.toString();
  }

}

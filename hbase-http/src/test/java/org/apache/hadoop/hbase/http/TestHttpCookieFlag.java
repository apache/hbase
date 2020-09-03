/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.hbase.http;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.HttpCookie;
import java.net.URI;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.security.GeneralSecurityException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;

import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class})
public class TestHttpCookieFlag {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHttpCookieFlag.class);

  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" +
      org.apache.hadoop.hbase.http.TestHttpCookieFlag.class.getSimpleName();
  private static String keystoresDir;
  private static String sslConfDir;
  private static SSLFactory clientSslFactory;
  private static HttpServer server;

  public static class DummyAuthenticationFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain) throws IOException,
                                                   ServletException {
      HttpServletResponse resp = (HttpServletResponse) response;
      boolean isHttps = "https".equals(request.getScheme());
      AuthenticationFilter.createAuthCookie(resp, "token", null, null, -1,
              true, isHttps);
      chain.doFilter(request, resp);
    }

    @Override
    public void destroy() {
    }
  }
  public static class DummyFilterInitializer extends FilterInitializer {
    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      container.addFilter("DummyAuth", DummyAuthenticationFilter.class
              .getName(), null);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY,
            DummyFilterInitializer.class.getName());
    conf.setInt("hbase.http.max.threads", 19); /* acceptors=2 + selectors=16 + request=1 */
    System.setProperty("hadoop.log.dir", BASEDIR); /* needed for /logs  */

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    Configuration sslConf = KeyStoreTestUtil.getSslConfig();

    clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, sslConf);
    clientSslFactory.init();

    server = new HttpServer.Builder()
            .setName("test")
            .addEndpoint(new URI("http://localhost"))
            .addEndpoint(new URI("https://localhost"))
            .setConf(conf)
            .keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
            .keyStore(sslConf.get("ssl.server.keystore.location"),
                    sslConf.get("ssl.server.keystore.password"),
                    sslConf.get("ssl.server.keystore.type", "jks"))
            .trustStore(sslConf.get("ssl.server.truststore.location"),
                    sslConf.get("ssl.server.truststore.password"),
                    sslConf.get("ssl.server.truststore.type", "jks"))
            .build();
    server.addPrivilegedServlet("echo", "/echo", TestHttpServer.EchoServlet.class);
    server.start();
  }

  @Test
  public void testHttpCookie() throws IOException {
    URL base = new URL("http://" + NetUtils.getHostPortString(server
            .getConnectorAddress(0)));
    HttpURLConnection conn = (HttpURLConnection) new URL(base,
            "/echo").openConnection();

    String header = conn.getHeaderField("Set-Cookie");
    Assert.assertTrue(header != null);
    List<HttpCookie> cookies = HttpCookie.parse(header);
    Assert.assertTrue(!cookies.isEmpty());
    Assert.assertTrue(header.contains("; HttpOnly"));
    Assert.assertTrue("token".equals(cookies.get(0).getValue()));
  }

  @Test
  public void testHttpsCookie() throws IOException, GeneralSecurityException {
    URL base = new URL("https://" + NetUtils.getHostPortString(server
            .getConnectorAddress(1)));
    HttpsURLConnection conn = (HttpsURLConnection) new URL(base,
            "/echo").openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());

    String header = conn.getHeaderField("Set-Cookie");
    Assert.assertTrue(header != null);

    List<HttpCookie> cookies = HttpCookie.parse(header);
    Assert.assertTrue(!cookies.isEmpty());
    Assert.assertTrue(header.contains("; HttpOnly"));
    Assert.assertTrue(cookies.get(0).getSecure());
    Assert.assertTrue("token".equals(cookies.get(0).getValue()));
  }

  @Test
  public void testHttpsCookieDefaultServlets() throws Exception {
    HttpsURLConnection conn = null;

    URL base = new URL("https://" + NetUtils.getHostPortString(server
        .getConnectorAddress(1)) + "/");

    for (String servlet : new String[] { "static",  "stacks", "logLevel", "jmx", "logs" }) {
      conn = (HttpsURLConnection) new URL(base,
          "/" + servlet).openConnection();
      conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());

      String header = conn.getHeaderField("Set-Cookie");
      Assert.assertTrue(header != null);
      List<HttpCookie> cookies = HttpCookie.parse(header);
      Assert.assertTrue(!cookies.isEmpty());
      Assert.assertTrue(header.contains("; HttpOnly"));
      Assert.assertTrue(cookies.get(0).getSecure());
      Assert.assertTrue("token".equals(cookies.get(0).getValue()));
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    server.stop();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
    clientSslFactory.destroy();
  }
}

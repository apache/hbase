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
package org.apache.hadoop.hbase.http;

import static org.hamcrest.Matchers.greaterThan;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.http.HttpServer.QuotingInputFilter.RequestQuoter;
import org.apache.hadoop.hbase.http.resource.JerseyResource;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.ajax.JSON;

@Category({MiscTests.class, SmallTests.class})
public class TestHttpServer extends HttpServerFunctionalTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHttpServer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHttpServer.class);
  private static HttpServer server;
  private static URL baseUrl;
  // jetty 9.4.x needs this many threads to start, even in the small.
  static final int MAX_THREADS = 16;

  @SuppressWarnings("serial")
  public static class EchoMapServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      PrintWriter out = response.getWriter();
      Map<String, String[]> params = request.getParameterMap();
      SortedSet<String> keys = new TreeSet<>(params.keySet());
      for(String key: keys) {
        out.print(key);
        out.print(':');
        String[] values = params.get(key);
        if (values.length > 0) {
          out.print(values[0]);
          for(int i=1; i < values.length; ++i) {
            out.print(',');
            out.print(values[i]);
          }
        }
        out.print('\n');
      }
      out.close();
    }
  }

  @SuppressWarnings("serial")
  public static class EchoServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
      PrintWriter out = response.getWriter();
      SortedSet<String> sortedKeys = new TreeSet<>();
      Enumeration<String> keys = request.getParameterNames();
      while(keys.hasMoreElements()) {
        sortedKeys.add(keys.nextElement());
      }
      for(String key: sortedKeys) {
        out.print(key);
        out.print(':');
        out.print(request.getParameter(key));
        out.print('\n');
      }
      out.close();
    }
  }

  @SuppressWarnings("serial")
  public static class LongHeaderServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
      Assert.assertEquals(63 * 1024, request.getHeader("longheader").length());
      response.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @SuppressWarnings("serial")
  public static class HtmlContentServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      response.setContentType("text/html");
      PrintWriter out = response.getWriter();
      out.print("hello world");
      out.close();
    }
  }

  @BeforeClass public static void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HttpServer.HTTP_MAX_THREADS, MAX_THREADS);
    server = createTestServer(conf);
    server.addUnprivilegedServlet("echo", "/echo", EchoServlet.class);
    server.addUnprivilegedServlet("echomap", "/echomap", EchoMapServlet.class);
    server.addUnprivilegedServlet("htmlcontent", "/htmlcontent", HtmlContentServlet.class);
    server.addUnprivilegedServlet("longheader", "/longheader", LongHeaderServlet.class);
    server.addJerseyResourcePackage(
        JerseyResource.class.getPackage().getName(), "/jersey/*");
    server.start();
    baseUrl = getServerURL(server);
    LOG.info("HTTP server started: "+ baseUrl);
  }

  @AfterClass public static void cleanup() throws Exception {
    server.stop();
  }

  /**
   * Test the maximum number of threads cannot be exceeded.
   */
  @Test
  public void testMaxThreads() throws Exception {
    int clientThreads = MAX_THREADS * 10;
    Executor executor = Executors.newFixedThreadPool(clientThreads);
    // Run many clients to make server reach its maximum number of threads
    final CountDownLatch ready = new CountDownLatch(clientThreads);
    final CountDownLatch start = new CountDownLatch(1);
    for (int i = 0; i < clientThreads; i++) {
      executor.execute(() -> {
        ready.countDown();
        try {
          start.await();
          assertEquals("a:b\nc:d\n",
                       readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
          int serverThreads = server.webServer.getThreadPool().getThreads();
          assertTrue("More threads are started than expected, Server Threads count: "
                  + serverThreads, serverThreads <= MAX_THREADS);
          LOG.info("Number of threads = " + serverThreads +
              " which is less or equal than the max = " + MAX_THREADS);
        } catch (Exception e) {
          // do nothing
        }
      });
    }
    // Start the client threads when they are all ready
    ready.await();
    start.countDown();
  }

  @Test public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n",
                 readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n",
                 readOutput(new URL(baseUrl, "/echo?a=b&c<=d&e=>")));
  }

  /** Test the echo map servlet that uses getParameterMap. */
  @Test public void testEchoMap() throws Exception {
    assertEquals("a:b\nc:d\n",
                 readOutput(new URL(baseUrl, "/echomap?a=b&c=d")));
    assertEquals("a:b,&gt;\nc&lt;:d\n",
                 readOutput(new URL(baseUrl, "/echomap?a=b&c<=d&a=>")));
  }

  /**
   *  Test that verifies headers can be up to 64K long.
   *  The test adds a 63K header leaving 1K for other headers.
   *  This is because the header buffer setting is for ALL headers,
   *  names and values included. */
  @Test public void testLongHeader() throws Exception {
    URL url = new URL(baseUrl, "/longheader");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    StringBuilder sb = new StringBuilder();
    for (int i = 0 ; i < 63 * 1024; i++) {
      sb.append("a");
    }
    conn.setRequestProperty("longheader", sb.toString());
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  @Test
  public void testContentTypes() throws Exception {
    // Static CSS files should have text/css
    URL cssUrl = new URL(baseUrl, "/static/test.css");
    HttpURLConnection conn = (HttpURLConnection)cssUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/css", conn.getContentType());

    // Servlets should have text/plain with proper encoding by default
    URL servletUrl = new URL(baseUrl, "/echo?a=b");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/plain;charset=utf-8", conn.getContentType());

    // We should ignore parameters for mime types - ie a parameter
    // ending in .css should not change mime type
    servletUrl = new URL(baseUrl, "/echo?a=b.css");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/plain;charset=utf-8", conn.getContentType());

    // Servlets that specify text/html should get that content type
    servletUrl = new URL(baseUrl, "/htmlcontent");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/html;charset=utf-8", conn.getContentType());

    // JSPs should default to text/html with utf8
    // JSPs do not work from unit tests
    // servletUrl = new URL(baseUrl, "/testjsp.jsp");
    // conn = (HttpURLConnection)servletUrl.openConnection();
    // conn.connect();
    // assertEquals(200, conn.getResponseCode());
    // assertEquals("text/html; charset=utf-8", conn.getContentType());
  }

  @Test
  public void testNegotiatesEncodingGzip() throws IOException {
    final InputStream stream = ClassLoader.getSystemResourceAsStream("webapps/static/test.css");
    assertNotNull(stream);
    final String sourceContent = readFully(stream);

    try (final CloseableHttpClient client = HttpClients.createMinimal()) {
      final HttpGet request = new HttpGet(new URL(baseUrl, "/static/test.css").toString());

      request.setHeader(HttpHeaders.ACCEPT_ENCODING, null);
      final long unencodedContentLength;
      try (final CloseableHttpResponse response = client.execute(request)) {
        final HttpEntity entity = response.getEntity();
        assertNotNull(entity);
        assertNull(entity.getContentEncoding());
        unencodedContentLength = entity.getContentLength();
        MatcherAssert.assertThat(unencodedContentLength, greaterThan(0L));
        final String unencodedEntityBody = readFully(entity.getContent());
        assertEquals(sourceContent, unencodedEntityBody);
      }

      request.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");
      final long encodedContentLength;
      try (final CloseableHttpResponse response = client.execute(request)) {
        final HttpEntity entity = response.getEntity();
        assertNotNull(entity);
        assertNotNull(entity.getContentEncoding());
        assertEquals("gzip", entity.getContentEncoding().getValue());
        encodedContentLength = entity.getContentLength();
        MatcherAssert.assertThat(encodedContentLength, greaterThan(0L));
        final String encodedEntityBody = readFully(entity.getContent());
        // the encoding/decoding process, as implemented in this specific combination of dependency
        // versions, does not perfectly preserve trailing whitespace. thus, `trim()`.
        assertEquals(sourceContent.trim(), encodedEntityBody.trim());
      }
      MatcherAssert.assertThat(unencodedContentLength, greaterThan(encodedContentLength));
    }
  }

  private static String readFully(final InputStream input) throws IOException {
    // TODO: when the time comes, delete me and replace with a JDK11 IO helper API.
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
      final StringBuilder sb = new StringBuilder();
      final CharBuffer buffer = CharBuffer.allocate(1024 * 2);
      while (reader.read(buffer) > 0) {
        sb.append(buffer);
        buffer.clear();
      }
      return sb.toString();
    } finally {
      input.close();
    }
  }

  /**
   * Dummy filter that mimics as an authentication filter. Obtains user identity
   * from the request parameter user.name. Wraps around the request so that
   * request.getRemoteUser() returns the user identity.
   *
   */
  public static class DummyServletFilter implements Filter {
    @Override
    public void destroy() { }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain filterChain) throws IOException, ServletException {
      final String userName = request.getParameter("user.name");
      ServletRequest requestModified =
        new HttpServletRequestWrapper((HttpServletRequest) request) {
        @Override
        public String getRemoteUser() {
          return userName;
        }
      };
      filterChain.doFilter(requestModified, response);
    }

    @Override
    public void init(FilterConfig arg0) { }
  }

  /**
   * FilterInitializer that initialized the DummyFilter.
   *
   */
  public static class DummyFilterInitializer extends FilterInitializer {
    public DummyFilterInitializer() {
    }

    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      container.addFilter("DummyFilter", DummyServletFilter.class.getName(), null);
    }
  }

  /**
   * Access a URL and get the corresponding return Http status code. The URL
   * will be accessed as the passed user, by sending user.name request
   * parameter.
   *
   * @param urlstring The url to access
   * @param userName The user to perform access as
   * @return The HTTP response code
   * @throws IOException if there is a problem communicating with the server
   */
  private static int getHttpStatusCode(String urlstring, String userName) throws IOException {
    URL url = new URL(urlstring + "?user.name=" + userName);
    System.out.println("Accessing " + url + " as user " + userName);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.connect();
    return connection.getResponseCode();
  }

  /**
   * Custom user->group mapping service.
   */
  public static class MyGroupsProvider extends ShellBasedUnixGroupsMapping {
    static Map<String, List<String>> mapping = new HashMap<>();

    static void clearMapping() {
      mapping.clear();
    }

    @Override
    public List<String> getGroups(String user) {
      return mapping.get(user);
    }
  }

  /**
   * Verify the access for /logs, /stacks, /conf, /logLevel and /metrics
   * servlets, when authentication filters are set, but authorization is not
   * enabled.
   */
  @Test
  @Ignore
  public void testDisabledAuthorizationOfDefaultServlets() throws Exception {
    Configuration conf = new Configuration();

    // Authorization is disabled by default
    conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY,
        DummyFilterInitializer.class.getName());
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Collections.singletonList("groupA"));
    MyGroupsProvider.mapping.put("userB", Collections.singletonList("groupB"));

    HttpServer myServer = new HttpServer.Builder().setName("test")
        .addEndpoint(new URI("http://localhost:0")).setFindPort(true).build();
    myServer.setAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();
    String serverURL = "http://" + NetUtils.getHostPortString(myServer.getConnectorAddress(0)) + "/";
    for (String servlet : new String[] { "conf", "logs", "stacks", "logLevel", "metrics" }) {
      for (String user : new String[] { "userA", "userB" }) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
    }
    myServer.stop();
  }

  /**
   * Verify the administrator access for /logs, /stacks, /conf, /logLevel and
   * /metrics servlets.
   */
  @Test
  @Ignore
  public void testAuthorizationOfDefaultServlets() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
        true);
    conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY,
        DummyFilterInitializer.class.getName());

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Collections.singletonList("groupA"));
    MyGroupsProvider.mapping.put("userB", Collections.singletonList("groupB"));
    MyGroupsProvider.mapping.put("userC", Collections.singletonList("groupC"));
    MyGroupsProvider.mapping.put("userD", Collections.singletonList("groupD"));
    MyGroupsProvider.mapping.put("userE", Collections.singletonList("groupE"));

    HttpServer myServer = new HttpServer.Builder().setName("test")
        .addEndpoint(new URI("http://localhost:0")).setFindPort(true).setConf(conf)
        .setACL(new AccessControlList("userA,userB groupC,groupD")).build();
    myServer.setAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();

    String serverURL = "http://"
        + NetUtils.getHostPortString(myServer.getConnectorAddress(0)) + "/";
    for (String servlet : new String[] { "conf", "logs", "stacks", "logLevel", "metrics" }) {
      for (String user : new String[] { "userA", "userB", "userC", "userD" }) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, getHttpStatusCode(
          serverURL + servlet, "userE"));
    }
    myServer.stop();
  }

  @Test
  public void testRequestQuoterWithNull() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn(null).when(request).getParameterValues("dummy");
    RequestQuoter requestQuoter = new RequestQuoter(request);
    String[] parameterValues = requestQuoter.getParameterValues("dummy");
    Assert.assertNull("It should return null "
            + "when there are no values for the parameter", parameterValues);
  }

  @Test
  public void testRequestQuoterWithNotNull() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    String[] values = new String[] { "abc", "def" };
    Mockito.doReturn(values).when(request).getParameterValues("dummy");
    RequestQuoter requestQuoter = new RequestQuoter(request);
    String[] parameterValues = requestQuoter.getParameterValues("dummy");
    Assert.assertTrue("It should return Parameter Values", Arrays.equals(
        values, parameterValues));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parse(String jsonString) {
    return (Map<String, Object>)JSON.parse(jsonString);
  }

  @Test public void testJersey() throws Exception {
    LOG.info("BEGIN testJersey()");
    final String js = readOutput(new URL(baseUrl, "/jersey/foo?op=bar"));
    final Map<String, Object> m = parse(js);
    LOG.info("m=" + m);
    assertEquals("foo", m.get(JerseyResource.PATH));
    assertEquals("bar", m.get(JerseyResource.OP));
    LOG.info("END testJersey()");
  }

  @Test
  public void testHasAdministratorAccess() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(null);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteUser()).thenReturn(null);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    //authorization OFF
    Assert.assertTrue(HttpServer.hasAdministratorAccess(context, request, response));

    //authorization ON & user NULL
    response = Mockito.mock(HttpServletResponse.class);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    Assert.assertFalse(HttpServer.hasAdministratorAccess(context, request, response));
    Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED),
            Mockito.anyString());

    //authorization ON & user NOT NULL & ACLs NULL
    response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getRemoteUser()).thenReturn("foo");
    Assert.assertTrue(HttpServer.hasAdministratorAccess(context, request, response));

    //authorization ON & user NOT NULL & ACLs NOT NULL & user not in ACLs
    response = Mockito.mock(HttpServletResponse.class);
    AccessControlList acls = Mockito.mock(AccessControlList.class);
    Mockito.when(acls.isUserAllowed(Mockito.<UserGroupInformation>any())).thenReturn(false);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(acls);
    Assert.assertFalse(HttpServer.hasAdministratorAccess(context, request, response));
    Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
            Mockito.anyString());

    //authorization ON & user NOT NULL & ACLs NOT NULL & user in in ACLs
    response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(acls.isUserAllowed(Mockito.<UserGroupInformation>any())).thenReturn(true);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(acls);
    Assert.assertTrue(HttpServer.hasAdministratorAccess(context, request, response));

  }

  @Test
  public void testRequiresAuthorizationAccess() throws Exception {
    Configuration conf = new Configuration();
    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    //requires admin access to instrumentation, FALSE by default
    Assert.assertTrue(HttpServer.isInstrumentationAccessAllowed(context, request, response));

    //requires admin access to instrumentation, TRUE
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN, true);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    AccessControlList acls = Mockito.mock(AccessControlList.class);
    Mockito.when(acls.isUserAllowed(Mockito.<UserGroupInformation>any())).thenReturn(false);
    Mockito.when(context.getAttribute(HttpServer.ADMINS_ACL)).thenReturn(acls);
    Assert.assertFalse(HttpServer.isInstrumentationAccessAllowed(context, request, response));
  }

  @Test
  public void testBindAddress() throws Exception {
    checkBindAddress("localhost", 0, false).stop();
    // hang onto this one for a bit more testing
    HttpServer myServer = checkBindAddress("localhost", 0, false);
    HttpServer myServer2 = null;
    try {
      int port = myServer.getConnectorAddress(0).getPort();
      // it's already in use, true = expect a higher port
      myServer2 = checkBindAddress("localhost", port, true);
      // try to reuse the port
      port = myServer2.getConnectorAddress(0).getPort();
      myServer2.stop();
      assertNull(myServer2.getConnectorAddress(0)); // not bound
      myServer2.openListeners();
      assertEquals(port, myServer2.getConnectorAddress(0).getPort()); // expect same port
    } finally {
      myServer.stop();
      if (myServer2 != null) {
        myServer2.stop();
      }
    }
  }

  private HttpServer checkBindAddress(String host, int port, boolean findPort)
      throws Exception {
    HttpServer server = createServer(host, port);
    try {
      // not bound, ephemeral should return requested port (0 for ephemeral)
      ServerConnector listener = server.getServerConnectors().get(0);

      assertEquals(port, listener.getPort());
      // verify hostname is what was given
      server.openListeners();
      assertEquals(host, server.getConnectorAddress(0).getHostName());

      int boundPort = server.getConnectorAddress(0).getPort();
      if (port == 0) {
        assertTrue(boundPort != 0); // ephemeral should now return bound port
      } else if (findPort) {
        assertTrue(boundPort > port);
        // allow a little wiggle room to prevent random test failures if
        // some consecutive ports are already in use
        assertTrue(boundPort - port < 8);
      }
    } catch (Exception e) {
      server.stop();
      throw e;
    }
    return server;
  }

  @Test
  public void testXFrameHeaderSameOrigin() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hbase.http.filter.xframeoptions.mode", "SAMEORIGIN");

    HttpServer myServer = new HttpServer.Builder().setName("test")
            .addEndpoint(new URI("http://localhost:0"))
            .setFindPort(true).setConf(conf).build();
    myServer.setAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.addUnprivilegedServlet("echo", "/echo", EchoServlet.class);
    myServer.start();

    String serverURL = "http://"
            + NetUtils.getHostPortString(myServer.getConnectorAddress(0));
    URL url = new URL(new URL(serverURL), "/echo?a=b&c=d");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals("SAMEORIGIN", conn.getHeaderField("X-Frame-Options"));
    myServer.stop();
  }

  @Test
  public void testNoCacheHeader() throws Exception {
    URL url = new URL(baseUrl, "/echo?a=b&c=d");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals("no-cache", conn.getHeaderField("Cache-Control"));
    assertEquals("no-cache", conn.getHeaderField("Pragma"));
    assertNotNull(conn.getHeaderField("Expires"));
    assertNotNull(conn.getHeaderField("Date"));
    assertEquals(conn.getHeaderField("Expires"), conn.getHeaderField("Date"));
    assertEquals("DENY", conn.getHeaderField("X-Frame-Options"));
  }

  @Test
  public void testHttpMethods() throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    URL url = new URL(baseUrl, "/echo?a=b");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("TRACE");
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }
}

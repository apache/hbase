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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.http.HttpServer.Builder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a base class for functional tests of the {@link HttpServer}. The methods are static for
 * other classes to import statically.
 */
public class HttpServerFunctionalTest extends Assert {
  private static final Logger LOG = LoggerFactory.getLogger(HttpServerFunctionalTest.class);
  /** JVM property for the webapp test dir : {@value} */
  public static final String TEST_BUILD_WEBAPPS = "test.build.webapps";
  /** expected location of the test.build.webapps dir: {@value} */
  private static final String BUILD_WEBAPPS_DIR = "src/test/resources/webapps";

  /** name of the test webapp: {@value} */
  private static final String TEST = "test";

  /**
   * Create but do not start the test webapp server. The test webapp dir is prepared/checked in
   * advance.
   * @return the server instance
   * @throws IOException    if a problem occurs
   * @throws AssertionError if a condition was not met
   */
  public static HttpServer createTestServer() throws IOException {
    prepareTestWebapp();
    return createServer(TEST);
  }

  /**
   * Create but do not start the test webapp server. The test webapp dir is prepared/checked in
   * advance.
   * @param conf the server configuration to use
   * @return the server instance
   * @throws IOException    if a problem occurs
   * @throws AssertionError if a condition was not met
   */
  public static HttpServer createTestServer(Configuration conf) throws IOException {
    prepareTestWebapp();
    return createServer(TEST, conf);
  }

  public static HttpServer createTestServer(Configuration conf, AccessControlList adminsAcl)
    throws IOException {
    prepareTestWebapp();
    return createServer(TEST, conf, adminsAcl);
  }

  /**
   * Create but do not start the test webapp server. The test webapp dir is prepared/checked in
   * advance.
   * @param conf the server configuration to use
   * @return the server instance
   * @throws IOException    if a problem occurs
   * @throws AssertionError if a condition was not met
   */
  public static HttpServer createTestServer(Configuration conf, String[] pathSpecs)
    throws IOException {
    prepareTestWebapp();
    return createServer(TEST, conf, pathSpecs);
  }

  public static HttpServer createTestServerWithSecurity(Configuration conf) throws IOException {
    prepareTestWebapp();
    return localServerBuilder(TEST).setFindPort(true).setConf(conf).setSecurityEnabled(true)
      // InfoServer normally sets these for us
      .setUsernameConfKey(HttpServer.HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_KEY)
      .setKeytabConfKey(HttpServer.HTTP_SPNEGO_AUTHENTICATION_KEYTAB_KEY).build();
  }

  public static HttpServer createTestServerWithSecurityAndAcl(Configuration conf,
    AccessControlList acl) throws IOException {
    prepareTestWebapp();
    return localServerBuilder(TEST).setFindPort(true).setConf(conf).setSecurityEnabled(true)
      // InfoServer normally sets these for us
      .setUsernameConfKey(HttpServer.HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_KEY)
      .setKeytabConfKey(HttpServer.HTTP_SPNEGO_AUTHENTICATION_KEYTAB_KEY).setSecurityEnabled(true)
      .setACL(acl).build();
  }

  /**
   * Prepare the test webapp by creating the directory from the test properties fail if the
   * directory cannot be created.
   * @throws AssertionError if a condition was not met
   */
  protected static void prepareTestWebapp() {
    String webapps = System.getProperty(TEST_BUILD_WEBAPPS, BUILD_WEBAPPS_DIR);
    File testWebappDir = new File(webapps + File.separatorChar + TEST);
    try {
      if (!testWebappDir.exists()) {
        fail("Test webapp dir " + testWebappDir.getCanonicalPath() + " missing");
      }
    } catch (IOException e) {
    }
  }

  /**
   * Create an HttpServer instance on the given address for the given webapp
   * @param host to bind
   * @param port to bind
   * @return the server
   * @throws IOException if it could not be created
   */
  public static HttpServer createServer(String host, int port) throws IOException {
    prepareTestWebapp();
    return new HttpServer.Builder().setName(TEST)
      .addEndpoint(URI.create("http://" + host + ":" + port)).setFindPort(true).build();
  }

  /**
   * Create an HttpServer instance for the given webapp
   * @param webapp the webapp to work with
   * @return the server
   * @throws IOException if it could not be created
   */
  public static HttpServer createServer(String webapp) throws IOException {
    return localServerBuilder(webapp).setFindPort(true).build();
  }

  /**
   * Create an HttpServer instance for the given webapp
   * @param webapp the webapp to work with
   * @param conf   the configuration to use for the server
   * @return the server
   * @throws IOException if it could not be created
   */
  public static HttpServer createServer(String webapp, Configuration conf) throws IOException {
    return localServerBuilder(webapp).setFindPort(true).setConf(conf).build();
  }

  public static HttpServer createServer(String webapp, Configuration conf,
    AccessControlList adminsAcl) throws IOException {
    return localServerBuilder(webapp).setFindPort(true).setConf(conf).setACL(adminsAcl).build();
  }

  private static Builder localServerBuilder(String webapp) {
    return new HttpServer.Builder().setName(webapp).addEndpoint(URI.create("http://localhost:0"));
  }

  /**
   * Create an HttpServer instance for the given webapp
   * @param webapp    the webapp to work with
   * @param conf      the configuration to use for the server
   * @param pathSpecs the paths specifications the server will service
   * @return the server
   * @throws IOException if it could not be created
   */
  public static HttpServer createServer(String webapp, Configuration conf, String[] pathSpecs)
    throws IOException {
    return localServerBuilder(webapp).setFindPort(true).setConf(conf).setPathSpec(pathSpecs)
      .build();
  }

  /**
   * Create and start a server with the test webapp
   * @return the newly started server
   * @throws IOException    on any failure
   * @throws AssertionError if a condition was not met
   */
  public static HttpServer createAndStartTestServer() throws IOException {
    HttpServer server = createTestServer();
    server.start();
    return server;
  }

  /**
   * If the server is non null, stop it
   * @param server to stop
   * @throws Exception on any failure
   */
  public static void stop(HttpServer server) throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  /**
   * Pass in a server, return a URL bound to localhost and its port
   * @param server server
   * @return a URL bonded to the base of the server
   * @throws MalformedURLException if the URL cannot be created.
   */
  public static URL getServerURL(HttpServer server) throws MalformedURLException {
    assertNotNull("No server", server);
    return new URL("http://" + NetUtils.getHostPortString(server.getConnectorAddress(0)));
  }

  /**
   * Read in the content from a URL
   * @param url URL To read
   * @return the text from the output
   * @throws IOException if something went wrong
   */
  protected static String readOutput(URL url) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = url.openConnection().getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }

  /**
   * Recursively deletes a {@link File}.
   */
  protected static void deleteRecursively(File d) {
    if (d.isDirectory()) {
      for (String name : d.list()) {
        File child = new File(d, name);
        if (child.isFile()) {
          child.delete();
        } else {
          deleteRecursively(child);
        }
      }
    }
    d.delete();
  }

  /**
   * Picks a free port on the host by binding a Socket to '0'.
   */
  protected static int getFreePort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    try {
      s.setReuseAddress(true);
      int port = s.getLocalPort();
      return port;
    } finally {
      if (null != s) {
        s.close();
      }
    }
  }

  /**
   * access a url, ignoring some IOException such as the page does not exist
   */
  public static void access(String urlstring) throws IOException {
    URL url = new URL(urlstring);

    URLConnection connection = url.openConnection();
    connection.connect();

    try (BufferedReader in = new BufferedReader(
      new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
      for (; in.readLine() != null;) {
        continue;
      }
    } catch (IOException ioe) {
      LOG.info("Got exception: ", ioe);
    }
  }

}

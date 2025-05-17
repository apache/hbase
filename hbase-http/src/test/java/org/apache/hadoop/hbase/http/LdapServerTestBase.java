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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.codec.binary.Base64;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.http.resource.JerseyResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for setting up and testing an HTTP server with LDAP authentication.
 */
public class LdapServerTestBase extends HttpServerFunctionalTest {
  private static final Logger LOG = LoggerFactory.getLogger(LdapServerTestBase.class);

  @ClassRule
  public static CreateLdapServerRule ldapRule = new CreateLdapServerRule();

  protected static HttpServer server;
  protected static URL baseUrl;

  private static final String AUTH_TYPE = "Basic ";

  /**
   * Sets up the HTTP server with LDAP authentication before any tests are run.
   * @throws Exception if an error occurs during server setup
   */
  @BeforeClass
  public static void setupServer() throws Exception {
    Configuration conf = new Configuration();
    setLdapConfigurations(conf);

    server = createTestServer(conf);
    server.addUnprivilegedServlet("echo", "/echo", TestHttpServer.EchoServlet.class);
    server.addJerseyResourcePackage(JerseyResource.class.getPackage().getName(), "/jersey/*");
    server.start();

    baseUrl = getServerURL(server);
    LOG.info("HTTP server started: " + baseUrl);
  }

  /**
   * Stops the HTTP server after all tests are completed.
   * @throws Exception if an error occurs during server shutdown
   */
  @AfterClass
  public static void stopServer() throws Exception {
    try {
      if (null != server) {
        server.stop();
      }
    } catch (Exception e) {
      LOG.info("Failed to stop info server", e);
    }
  }

  /**
   * Configures the provided Configuration object for LDAP authentication.
   * @param conf the Configuration object to set LDAP properties on
   * @return the configured Configuration object
   */
  protected static Configuration setLdapConfigurations(Configuration conf) {
    conf.setInt(HttpServer.HTTP_MAX_THREADS, TestHttpServer.MAX_THREADS);

    // Enable LDAP (pre-req)
    conf.set(HttpServer.HTTP_UI_AUTHENTICATION, "ldap");
    conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY,
      "org.apache.hadoop.hbase.http.lib.AuthenticationFilterInitializer");
    conf.set("hadoop.http.authentication.type", "ldap");
    conf.set("hadoop.http.authentication.ldap.providerurl", String.format("ldap://%s:%s",
      LdapConstants.LDAP_SERVER_ADDR, ldapRule.getLdapServer().getPort()));
    conf.set("hadoop.http.authentication.ldap.enablestarttls", "false");
    conf.set("hadoop.http.authentication.ldap.basedn", LdapConstants.LDAP_BASE_DN);
    return conf;
  }

  /**
   * Generates a Basic Authentication header from the provided credentials.
   * @param credentials the credentials to encode
   * @return the Basic Authentication header
   */
  String getBasicAuthHeader(String credentials) {
    return AUTH_TYPE + new Base64(0).encodeToString(credentials.getBytes());
  }

  /**
   * Opens an HTTP connection to the specified endpoint with optional Basic Authentication.
   * @param endpoint    the endpoint to connect to
   * @param credentials the credentials for Basic Authentication (optional)
   * @return the opened HttpURLConnection
   * @throws IOException if an error occurs while opening the connection
   */
  protected HttpURLConnection openConnection(String endpoint, String credentials)
    throws IOException {
    URL url = new URL(getServerURL(server) + endpoint);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    if (credentials != null) {
      conn.setRequestProperty("Authorization", getBasicAuthHeader(credentials));
    }
    return conn;
  }
}

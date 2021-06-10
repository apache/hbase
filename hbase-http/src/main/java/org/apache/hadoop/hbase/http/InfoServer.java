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

import java.io.IOException;
import java.net.URI;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/stacks/" -&gt; points to stack trace
 *   "/static/" -&gt; points to common static files (src/hbase-webapps/static)
 *   "/" -&gt; the jsp server code from (src/hbase-webapps/&lt;name&gt;)
 */
@InterfaceAudience.Private
public class InfoServer {
  private static final String HBASE_APP_DIR = "hbase-webapps";
  private final org.apache.hadoop.hbase.http.HttpServer httpServer;

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/hbase-webapps/<code>name</code>.
   * @param name The name of the server
   * @param bindAddress address to bind to
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and increment by 1 until it
   *                 finds a free port.
   * @param c the {@link Configuration} to build the server
   * @throws IOException if getting one of the password fails or the server cannot be created
   */
  public InfoServer(String name, String bindAddress, int port, boolean findPort,
      final Configuration c) throws IOException {
    HttpConfig httpConfig = new HttpConfig(c);
    HttpServer.Builder builder =
      new org.apache.hadoop.hbase.http.HttpServer.Builder();

    builder.setName(name).addEndpoint(URI.create(httpConfig.getSchemePrefix() +
      HostAndPort.fromParts(bindAddress,port).toString())).
        setAppDir(HBASE_APP_DIR).setFindPort(findPort).setConf(c);
    String logDir = System.getProperty("hbase.log.dir");
    if (logDir != null) {
      builder.setLogDir(logDir);
    }
    if (httpConfig.isSecure()) {
      builder.keyPassword(HBaseConfiguration
              .getPassword(c, "ssl.server.keystore.keypassword", null))
        .keyStore(c.get("ssl.server.keystore.location"),
                HBaseConfiguration.getPassword(c,"ssl.server.keystore.password", null),
                c.get("ssl.server.keystore.type", "jks"))
        .trustStore(c.get("ssl.server.truststore.location"),
                HBaseConfiguration.getPassword(c, "ssl.server.truststore.password", null),
                c.get("ssl.server.truststore.type", "jks"));
      builder.excludeCiphers(c.get("ssl.server.exclude.cipher.list"));
    }
    // Enable SPNEGO authentication
    if ("kerberos".equalsIgnoreCase(c.get(HttpServer.HTTP_UI_AUTHENTICATION, null))) {
      builder.setUsernameConfKey(HttpServer.HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_KEY)
        .setKeytabConfKey(HttpServer.HTTP_SPNEGO_AUTHENTICATION_KEYTAB_KEY)
        .setKerberosNameRulesKey(HttpServer.HTTP_SPNEGO_AUTHENTICATION_KRB_NAME_KEY)
        .setSignatureSecretFileKey(
            HttpServer.HTTP_AUTHENTICATION_SIGNATURE_SECRET_FILE_KEY)
        .setSecurityEnabled(true);

      // Set an admin ACL on sensitive webUI endpoints
      AccessControlList acl = buildAdminAcl(c);
      builder.setACL(acl);
    }
    this.httpServer = builder.build();
  }

  /**
   * Builds an ACL that will restrict the users who can issue commands to endpoints on the UI
   * which are meant only for administrators.
   */
  AccessControlList buildAdminAcl(Configuration conf) {
    final String userGroups = conf.get(HttpServer.HTTP_SPNEGO_AUTHENTICATION_ADMIN_USERS_KEY, null);
    final String adminGroups = conf.get(
        HttpServer.HTTP_SPNEGO_AUTHENTICATION_ADMIN_GROUPS_KEY, null);
    if (userGroups == null && adminGroups == null) {
      // Backwards compatibility - if the user doesn't have anything set, allow all users in.
      return new AccessControlList("*", null);
    }
    return new AccessControlList(userGroups, adminGroups);
  }

  /**
   * Explicitly invoke {@link #addPrivilegedServlet(String, String, Class)} or
   * {@link #addUnprivilegedServlet(String, String, Class)} instead of this method.
   * This method will add a servlet which any authenticated user can access.
   *
   * @deprecated Use {@link #addUnprivilegedServlet(String, String, Class)} or
   *    {@link #addPrivilegedServlet(String, String, Class)} instead of this
   *    method which does not state outwardly what kind of authz rules will
   *    be applied to this servlet.
   */
  @Deprecated
  public void addServlet(String name, String pathSpec,
          Class<? extends HttpServlet> clazz) {
    addUnprivilegedServlet(name, pathSpec, clazz);
  }

  /**
   * @see HttpServer#addUnprivilegedServlet(String, String, Class)
   */
  public void addUnprivilegedServlet(String name, String pathSpec,
          Class<? extends HttpServlet> clazz) {
    this.httpServer.addUnprivilegedServlet(name, pathSpec, clazz);
  }

  /**
   * @see HttpServer#addPrivilegedServlet(String, String, Class)
   */
  public void addPrivilegedServlet(String name, String pathSpec,
          Class<? extends HttpServlet> clazz) {
    this.httpServer.addPrivilegedServlet(name, pathSpec, clazz);
  }

  public void setAttribute(String name, Object value) {
    this.httpServer.setAttribute(name, value);
  }

  public void start() throws IOException {
    this.httpServer.start();
  }

  /**
   * @return the port of the info server
   * @deprecated Since 0.99.0
   */
  @Deprecated
  public int getPort() {
    return this.httpServer.getPort();
  }

  public void stop() throws Exception {
    this.httpServer.stop();
  }


  /**
   * Returns true if and only if UI authentication (spnego) is enabled, UI authorization is enabled,
   * and the requesting user is defined as an administrator. If the UI is set to readonly, this
   * method always returns false.
   */
  public static boolean canUserModifyUI(
      HttpServletRequest req, ServletContext ctx, Configuration conf) {
    if (conf.getBoolean("hbase.master.ui.readonly", false)) {
      return false;
    }
    String remoteUser = req.getRemoteUser();
    if ("kerberos".equalsIgnoreCase(conf.get(HttpServer.HTTP_UI_AUTHENTICATION)) &&
        conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false) &&
        remoteUser != null) {
      return HttpServer.userHasAdministratorAccess(ctx, remoteUser);
    }
    return false;
  }
}

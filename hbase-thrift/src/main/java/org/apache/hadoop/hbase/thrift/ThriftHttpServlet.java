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

package org.apache.hadoop.hbase.thrift;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * Thrift Http Servlet is used for performing Kerberos authentication if security is enabled and
 * also used for setting the user specified in "doAs" parameter.
 */
@InterfaceAudience.Private
public class ThriftHttpServlet extends TServlet {
  private static final long serialVersionUID = 1L;
  public static final Log LOG = LogFactory.getLog(ThriftHttpServlet.class.getName());
  private transient final UserGroupInformation realUser;
  private transient final Configuration conf;
  private final boolean securityEnabled;
  private final boolean doAsEnabled;
  private transient ThriftServerRunner.HBaseHandler hbaseHandler;

  public ThriftHttpServlet(TProcessor processor, TProtocolFactory protocolFactory,
      UserGroupInformation realUser, Configuration conf, ThriftServerRunner.HBaseHandler
      hbaseHandler, boolean securityEnabled, boolean doAsEnabled) {
    super(processor, protocolFactory);
    this.realUser = realUser;
    this.conf = conf;
    this.hbaseHandler = hbaseHandler;
    this.securityEnabled = securityEnabled;
    this.doAsEnabled = doAsEnabled;
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String effectiveUser = realUser.getShortUserName();
    if (securityEnabled) {
      try {
        // As Thrift HTTP transport doesn't support SPNEGO yet (THRIFT-889),
        // Kerberos authentication is being done at servlet level.
        effectiveUser = doKerberosAuth(request);
      } catch (HttpAuthenticationException e) {
        LOG.error("Kerberos Authentication failed", e);
        // Send a 401 to the client
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.getWriter().println("Authentication Error: " + e.getMessage());
      }
    }
    String doAsUserFromQuery = request.getHeader("doAs");
    if (doAsUserFromQuery != null) {
      if (!doAsEnabled) {
        throw new ServletException("Support for proxyuser is not configured");
      }
      // create and attempt to authorize a proxy user (the client is attempting
      // to do proxy user)
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, realUser);
      // validate the proxy user authorization
      try {
        ProxyUsers.authorize(ugi, request.getRemoteAddr(), conf);
      } catch (AuthorizationException e) {
        throw new ServletException(e.getMessage());
      }
      effectiveUser = doAsUserFromQuery;
    }
    hbaseHandler.setEffectiveUser(effectiveUser);
    super.doPost(request, response);
  }

  /**
   * Do the GSS-API kerberos authentication.
   * We already have a logged in subject in the form of serviceUGI,
   * which GSS-API will extract information from.
   */
  private String doKerberosAuth(HttpServletRequest request)
      throws HttpAuthenticationException {
    try {
      return realUser.doAs(new HttpKerberosServerAction(request, realUser));
    } catch (Exception e) {
      LOG.error("Failed to perform authentication");
      throw new HttpAuthenticationException(e);
    }
  }


  private static class HttpKerberosServerAction implements PrivilegedExceptionAction<String> {
    HttpServletRequest request;
    UserGroupInformation serviceUGI;
    HttpKerberosServerAction(HttpServletRequest request, UserGroupInformation serviceUGI) {
      this.request = request;
      this.serviceUGI = serviceUGI;
    }

    @Override
    public String run() throws HttpAuthenticationException {
      // Get own Kerberos credentials for accepting connection
      GSSManager manager = GSSManager.getInstance();
      GSSContext gssContext = null;
      String serverPrincipal = SecurityUtil.getPrincipalWithoutRealm(serviceUGI.getUserName());
      try {
        // This Oid for Kerberos GSS-API mechanism.
        Oid kerberosMechOid = new Oid("1.2.840.113554.1.2.2");
        // Oid for SPNego GSS-API mechanism.
        Oid spnegoMechOid = new Oid("1.3.6.1.5.5.2");
        // Oid for kerberos principal name
        Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");
        // GSS name for server
        GSSName serverName = manager.createName(serverPrincipal, krb5PrincipalOid);
        // GSS credentials for server
        GSSCredential serverCreds = manager.createCredential(serverName,
            GSSCredential.DEFAULT_LIFETIME,
            new Oid[]{kerberosMechOid, spnegoMechOid},
            GSSCredential.ACCEPT_ONLY);
        // Create a GSS context
        gssContext = manager.createContext(serverCreds);
        // Get service ticket from the authorization header
         String serviceTicketBase64 = getAuthHeader(request);
         byte[] inToken = Base64.decodeBase64(serviceTicketBase64.getBytes());
         gssContext.acceptSecContext(inToken, 0, inToken.length);
         // Authenticate or deny based on its context completion
         if (!gssContext.isEstablished()) {
          throw new HttpAuthenticationException("Kerberos authentication failed: " +
              "unable to establish context with the service ticket " +
              "provided by the client.");
         }
         return SecurityUtil.getUserFromPrincipal(gssContext.getSrcName().toString());
      } catch (GSSException e) {
        throw new HttpAuthenticationException("Kerberos authentication failed: ", e);
      } finally {
        if (gssContext != null) {
          try {
            gssContext.dispose();
          } catch (GSSException e) {
            LOG.warn("Error while disposing GSS Context", e);
          }
        }
      }
    }

    /**
     * Returns the base64 encoded auth header payload
     *
     * @throws HttpAuthenticationException if a remote or network exception occurs
     */
    private String getAuthHeader(HttpServletRequest request)
        throws HttpAuthenticationException {
      String authHeader = request.getHeader("Authorization");
      // Each http request must have an Authorization header
      if (authHeader == null || authHeader.isEmpty()) {
        throw new HttpAuthenticationException("Authorization header received " +
            "from the client is empty.");
      }
      String authHeaderBase64String;
      int beginIndex = ("Negotiate ").length();
      authHeaderBase64String = authHeader.substring(beginIndex);
      // Authorization header must have a payload
      if (authHeaderBase64String == null || authHeaderBase64String.isEmpty()) {
        throw new HttpAuthenticationException("Authorization header received " +
            "from the client does not contain any data.");
      }
      return authHeaderBase64String;
    }
  }
}

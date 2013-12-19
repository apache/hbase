/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;

import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.conf.Configuration;

/**
 * REST servlet container. It is used to get the remote request user
 * without going through @HttpContext, so that we can minimize code changes.
 */
@InterfaceAudience.Private
public class RESTServletContainer extends ServletContainer {
  private static final long serialVersionUID = -2474255003443394314L;

  /**
   * This container is used only if authentication and
   * impersonation is enabled. The remote request user is used
   * as a proxy user for impersonation in invoking any REST service.
   */
  @Override
  public void service(final HttpServletRequest request,
      final HttpServletResponse response) throws ServletException, IOException {
    final String doAsUserFromQuery = request.getParameter("doAs");
    Configuration conf = RESTServlet.getInstance().getConfiguration();
    final boolean proxyConfigured = conf.getBoolean("hbase.rest.support.proxyuser", false);
    if (doAsUserFromQuery != null && !proxyConfigured) {
      throw new ServletException("Support for proxyuser is not configured");
    }
    UserGroupInformation ugi = RESTServlet.getInstance().getRealUser();
    if (doAsUserFromQuery != null) {
      // create and attempt to authorize a proxy user (the client is attempting
      // to do proxy user)
      ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);    
      // validate the proxy user authorization
      try {
        ProxyUsers.authorize(ugi, request.getRemoteAddr(), conf);
      } catch(AuthorizationException e) {
        throw new ServletException(e.getMessage());
      }
    } else {
      // the REST server would send the request without validating the proxy
      // user authorization
      ugi = UserGroupInformation.createProxyUser(request.getRemoteUser(), ugi);
    }
    RESTServlet.getInstance().setEffectiveUser(ugi);
    super.service(request, response);
  }
}

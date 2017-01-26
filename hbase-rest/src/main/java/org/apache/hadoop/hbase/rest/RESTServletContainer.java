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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

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
    RESTServlet servlet = RESTServlet.getInstance();
    if (doAsUserFromQuery != null) {
      Configuration conf = servlet.getConfiguration();
      if (!servlet.supportsProxyuser()) {
        throw new ServletException("Support for proxyuser is not configured");
      }
      // Authenticated remote user is attempting to do 'doAs' proxy user.
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(request.getRemoteUser());
      // create and attempt to authorize a proxy user (the client is attempting
      // to do proxy user)
      ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);
      // validate the proxy user authorization
      try {
        ProxyUsers.authorize(ugi, request.getRemoteAddr(), conf);
      } catch(AuthorizationException e) {
        throw new ServletException(e.getMessage());
      }
      servlet.setEffectiveUser(doAsUserFromQuery);
    } else {
      String effectiveUser = request.getRemoteUser();
      servlet.setEffectiveUser(effectiveUser);
    }
    super.service(request, response);
  }
}

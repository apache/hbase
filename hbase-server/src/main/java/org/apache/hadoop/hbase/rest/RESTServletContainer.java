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
    RESTServlet.getInstance().setEffectiveUser(request.getRemoteUser());
    super.service(request, response);
  }
}

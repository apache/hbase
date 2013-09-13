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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DummyFilter implements Filter {
  private Log LOG = LogFactory.getLog(getClass());

  @Override
  public void destroy() {
  }

  @Override
  public void doFilter(ServletRequest paramServletRequest, ServletResponse paramServletResponse,
      FilterChain paramFilterChain) throws IOException, ServletException {
    if (paramServletRequest instanceof HttpServletRequest
        && paramServletResponse instanceof HttpServletResponse) {
      HttpServletRequest request = (HttpServletRequest) paramServletRequest;
      HttpServletResponse response = (HttpServletResponse) paramServletResponse;

      String path = request.getRequestURI();
      LOG.info(path);
      if (path.indexOf("/status/cluster") >= 0) {
        LOG.info("Blocking cluster status request");
        response.sendError(HttpServletResponse.SC_NOT_FOUND, "Cluster status cannot be requested.");
      } else {
        paramFilterChain.doFilter(request, response);
      }
    }
  }

  @Override
  public void init(FilterConfig filterChain) throws ServletException {
  }

}

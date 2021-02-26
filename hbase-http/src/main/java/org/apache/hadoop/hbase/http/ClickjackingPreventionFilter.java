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
import java.util.HashMap;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ClickjackingPreventionFilter implements Filter {
  private FilterConfig filterConfig;
  private static final String DEFAULT_XFRAMEOPTIONS = "DENY";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    this.filterConfig = filterConfig;
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
        throws IOException, ServletException {
    HttpServletResponse httpRes = (HttpServletResponse) res;
    httpRes.addHeader("X-Frame-Options", filterConfig.getInitParameter("xframeoptions"));
    chain.doFilter(req, res);
  }

  @Override
  public void destroy() {
  }

  public static Map<String, String> getDefaultParameters(Configuration conf) {
    Map<String, String> params = new HashMap<>();
    params.put("xframeoptions", conf.get("hbase.http.filter.xframeoptions.mode",
        DEFAULT_XFRAMEOPTIONS));
    return params;
  }
}

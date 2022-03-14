/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class SecurityHeadersFilter implements Filter {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecurityHeadersFilter.class);
  private static final String DEFAULT_HSTS = "max-age=63072000;includeSubDomains;preload";
  private static final String DEFAULT_CSP = "default-src https: data: 'unsafe-inline' 'unsafe-eval'";
  private FilterConfig filterConfig;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    this.filterConfig = filterConfig;
    LOG.info("Added security headers filter");
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    httpResponse.addHeader("X-Content-Type-Options", "nosniff");
    httpResponse.addHeader("X-XSS-Protection", "1; mode=block");
    String hsts = filterConfig.getInitParameter("hsts");
    if (StringUtils.isNotBlank(hsts)) {
      httpResponse.addHeader("Strict-Transport-Security", hsts);
    }
    String csp = filterConfig.getInitParameter("csp");
    if (StringUtils.isNotBlank(csp)) {
      httpResponse.addHeader("Content-Security-Policy", csp);
    }
    chain.doFilter(request, response);
  }

  @Override
  public void destroy() {
  }

  /**
   * @param conf configuration
   * @param isSecure use secure defaults if 'true'
   * @return default parameters, as a map
   */
  public static Map<String, String> getDefaultParameters(Configuration conf, boolean isSecure) {
    Map<String, String> params = new HashMap<>();
    params.put("hsts", conf.get("hbase.http.filter.hsts.value", isSecure ? DEFAULT_HSTS : ""));
    params.put("csp", conf.get("hbase.http.filter.csp.value", isSecure ? DEFAULT_CSP : ""));
    return params;
  }

  /**
   * @param conf configuration
   * @return default parameters, as a map
   * @deprecated Use {@link SecurityHeadersFilter#getDefaultParameters(Configuration, boolean)}
   *   instead.
   */
  @Deprecated
  public static Map<String, String> getDefaultParameters(Configuration conf) {
    return getDefaultParameters(conf, false);
  }

}

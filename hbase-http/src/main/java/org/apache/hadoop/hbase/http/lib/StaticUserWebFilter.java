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
package org.apache.hadoop.hbase.http.lib;

import static org.apache.hadoop.hbase.http.ServerConfigurationKeys.DEFAULT_HBASE_HTTP_STATIC_USER;
import static org.apache.hadoop.hbase.http.ServerConfigurationKeys.HBASE_HTTP_STATIC_USER;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.http.FilterContainer;
import org.apache.hadoop.hbase.http.FilterInitializer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a servlet filter that pretends to authenticate a fake user (Dr.Who)
 * so that the web UI is usable for a secure cluster without authentication.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class StaticUserWebFilter extends FilterInitializer {
  static final String DEPRECATED_UGI_KEY = "dfs.web.ugi";

  private static final Logger LOG = LoggerFactory.getLogger(StaticUserWebFilter.class);

  static class User implements Principal {
    private final String name;
    public User(String name) {
      this.name = name;
    }
    @Override
    public String getName() {
      return name;
    }
    @Override
    public int hashCode() {
      return name.hashCode();
    }
    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other == null || other.getClass() != getClass()) {
        return false;
      }
      return ((User) other).name.equals(name);
    }
    @Override
    public String toString() {
      return name;
    }
  }

  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  public static class StaticUserFilter implements Filter {
    private User user;
    private String username;

    @Override
    public void destroy() {
      // NOTHING
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain
                         ) throws IOException, ServletException {
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      // if the user is already authenticated, don't override it
      if (httpRequest.getRemoteUser() != null) {
        chain.doFilter(request, response);
      } else {
        HttpServletRequestWrapper wrapper =
            new HttpServletRequestWrapper(httpRequest) {
          @Override
          public Principal getUserPrincipal() {
            return user;
          }
          @Override
          public String getRemoteUser() {
            return username;
          }
        };
        chain.doFilter(wrapper, response);
      }
    }

    @Override
    public void init(FilterConfig conf) throws ServletException {
      this.username = conf.getInitParameter(HBASE_HTTP_STATIC_USER);
      this.user = new User(username);
    }

  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    HashMap<String, String> options = new HashMap<>();

    String username = getUsernameFromConf(conf);
    options.put(HBASE_HTTP_STATIC_USER, username);

    container.addFilter("static_user_filter",
                        StaticUserFilter.class.getName(),
                        options);
  }

  /**
   * Retrieve the static username from the configuration.
   */
  static String getUsernameFromConf(Configuration conf) {
    String oldStyleUgi = conf.get(DEPRECATED_UGI_KEY);
    if (oldStyleUgi != null) {
      // We can't use the normal configuration deprecation mechanism here
      // since we need to split out the username from the configured UGI.
      LOG.warn(DEPRECATED_UGI_KEY + " should not be used. Instead, use " +
          HBASE_HTTP_STATIC_USER + ".");
      String[] parts = oldStyleUgi.split(",");
      return parts[0];
    } else {
      return conf.get(HBASE_HTTP_STATIC_USER,
        DEFAULT_HBASE_HTTP_STATIC_USER);
    }
  }

}

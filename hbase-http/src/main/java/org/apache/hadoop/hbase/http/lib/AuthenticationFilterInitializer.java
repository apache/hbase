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
package org.apache.hadoop.hbase.http.lib;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.http.FilterContainer;
import org.apache.hadoop.hbase.http.FilterInitializer;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is copied from Hadoop. Initializes hadoop-auth AuthenticationFilter which provides
 * support for Kerberos HTTP SPNEGO authentication.
 * <p>
 * It enables anonymous access, simple/pseudo and Kerberos HTTP SPNEGO authentication for HBase web
 * UI endpoints.
 * <p>
 * Refer to the <code>core-default.xml</code> file, after the comment 'HTTP Authentication' for
 * details on the configuration options. All related configuration properties have
 * 'hadoop.http.authentication.' as prefix.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AuthenticationFilterInitializer extends FilterInitializer {

  static final String PREFIX = "hadoop.http.authentication.";

  /**
   * Initializes hadoop-auth AuthenticationFilter.
   * <p>
   * Propagates to hadoop-auth AuthenticationFilter configuration all Hadoop configuration
   * properties prefixed with "hadoop.http.authentication."
   * @param container The filter container
   * @param conf      Configuration for run-time parameters
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> filterConfig = getFilterConfigMap(conf, PREFIX);

    container.addFilter("authentication", AuthenticationFilter.class.getName(), filterConfig);
  }

  public static Map<String, String> getFilterConfigMap(Configuration conf, String prefix) {
    Map<String, String> filterConfig = new HashMap<String, String>();

    // setting the cookie path to root '/' so it is used for all resources.
    filterConfig.put(AuthenticationFilter.COOKIE_PATH, "/");
    Map<String, String> propsWithPrefix = conf.getPropsWithPrefix(prefix);

    for (Map.Entry<String, String> entry : propsWithPrefix.entrySet()) {
      filterConfig.put(entry.getKey(), entry.getValue());
    }

    // Resolve _HOST into bind address
    String bindAddress = conf.get(HttpServer.BIND_ADDRESS);
    String principal = filterConfig.get(KerberosAuthenticationHandler.PRINCIPAL);
    if (principal != null) {
      try {
        principal = SecurityUtil.getServerPrincipal(principal, bindAddress);
      } catch (IOException ex) {
        throw new RuntimeException("Could not resolve Kerberos principal name: " + ex.toString(),
          ex);
      }
      filterConfig.put(KerberosAuthenticationHandler.PRINCIPAL, principal);
    }
    return filterConfig;
  }

}

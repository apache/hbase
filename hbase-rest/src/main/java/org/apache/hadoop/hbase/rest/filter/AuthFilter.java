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
package org.apache.hadoop.hbase.rest.filter;

import static org.apache.hadoop.hbase.rest.Constants.REST_AUTHENTICATION_PRINCIPAL;
import static org.apache.hadoop.hbase.rest.Constants.REST_DNS_INTERFACE;
import static org.apache.hadoop.hbase.rest.Constants.REST_DNS_NAMESERVER;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

public class AuthFilter extends AuthenticationFilter {
  private static final Log LOG = LogFactory.getLog(AuthFilter.class);
  private static final String REST_PREFIX = "hbase.rest.authentication.";
  private static final int REST_PREFIX_LEN = REST_PREFIX.length();

  /**
   * Returns the configuration to be used by the authentication filter
   * to initialize the authentication handler.
   *
   * This filter retrieves all HBase configurations and passes those started
   * with REST_PREFIX to the authentication handler.  It is useful to support
   * plugging different authentication handlers.
  */
  @Override
  protected Properties getConfiguration(
      String configPrefix, FilterConfig filterConfig) throws ServletException {
    Properties props = super.getConfiguration(configPrefix, filterConfig);
    //setting the cookie path to root '/' so it is used for all resources.
    props.setProperty(AuthenticationFilter.COOKIE_PATH, "/");

    Configuration conf = HBaseConfiguration.create();
    for (Map.Entry<String, String> entry : conf) {
      String name = entry.getKey();
      if (name.startsWith(REST_PREFIX)) {
        String value = entry.getValue();
        if(name.equals(REST_AUTHENTICATION_PRINCIPAL))  {
          try {
            String machineName = Strings.domainNamePointerToHostName(
              DNS.getDefaultHost(conf.get(REST_DNS_INTERFACE, "default"),
                conf.get(REST_DNS_NAMESERVER, "default")));
            value = SecurityUtil.getServerPrincipal(value, machineName);
          } catch (IOException ie) {
            throw new ServletException("Failed to retrieve server principal", ie);
          }
        }
        LOG.debug("Setting property " + name + "=" + value);
        name = name.substring(REST_PREFIX_LEN);
        props.setProperty(name, value);
      }
    }
    return props;
  }
}

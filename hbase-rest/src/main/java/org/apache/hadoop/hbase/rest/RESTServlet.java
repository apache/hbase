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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.ConnectionCache;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
@InterfaceAudience.Private
public class RESTServlet implements Constants {
  private static final Logger LOG = LoggerFactory.getLogger(RESTServlet.class);
  private static RESTServlet INSTANCE;
  private final Configuration conf;
  private final MetricsREST metrics;
  private final ConnectionCache connectionCache;
  private final UserGroupInformation realUser;
  private final JvmPauseMonitor pauseMonitor;

  public static final String CLEANUP_INTERVAL = "hbase.rest.connection.cleanup-interval";
  public static final String MAX_IDLETIME = "hbase.rest.connection.max-idletime";
  static final String HBASE_REST_SUPPORT_PROXYUSER = "hbase.rest.support.proxyuser";

  UserGroupInformation getRealUser() {
    return realUser;
  }

  /**
   * @return the RESTServlet singleton instance
   */
  public synchronized static RESTServlet getInstance() {
    assert(INSTANCE != null);
    return INSTANCE;
  }

  /**
   * @return the ConnectionCache instance
   */
  public ConnectionCache getConnectionCache() {
    return connectionCache;
  }

  /**
   * @param conf Existing configuration to use in rest servlet
   * @param userProvider the login user provider
   * @return the RESTServlet singleton instance
   * @throws IOException
   */
  public synchronized static RESTServlet getInstance(Configuration conf,
      UserProvider userProvider) throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new RESTServlet(conf, userProvider);
    }
    return INSTANCE;
  }

  public synchronized static void stop() {
    if (INSTANCE != null) {
      INSTANCE.shutdown();
      INSTANCE = null;
    }
  }

  /**
   * Constructor with existing configuration
   * @param conf existing configuration
   * @param userProvider the login user provider
   * @throws IOException
   */
  RESTServlet(final Configuration conf,
      final UserProvider userProvider) throws IOException {
    this.realUser = userProvider.getCurrent().getUGI();
    this.conf = conf;
    registerCustomFilter(conf);

    int cleanInterval = conf.getInt(CLEANUP_INTERVAL, 10 * 1000);
    int maxIdleTime = conf.getInt(MAX_IDLETIME, 10 * 60 * 1000);
    connectionCache = new ConnectionCache(
      conf, userProvider, cleanInterval, maxIdleTime);
    if (supportsProxyuser()) {
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    }

    metrics = new MetricsREST();

    pauseMonitor = new JvmPauseMonitor(conf, metrics.getSource());
    pauseMonitor.start();
  }

  Admin getAdmin() throws IOException {
    return connectionCache.getAdmin();
  }

  /**
   * Caller closes the table afterwards.
   */
  Table getTable(String tableName) throws IOException {
    return connectionCache.getTable(tableName);
  }

  Configuration getConfiguration() {
    return conf;
  }

  MetricsREST getMetrics() {
    return metrics;
  }

  /**
   * Helper method to determine if server should
   * only respond to GET HTTP method requests.
   * @return boolean for server read-only state
   */
  boolean isReadOnly() {
    return getConfiguration().getBoolean("hbase.rest.readonly", false);
  }

  void setEffectiveUser(String effectiveUser) {
    connectionCache.setEffectiveUser(effectiveUser);
  }

  /**
   * Shutdown any services that need to stop
   */
  void shutdown() {
    if (pauseMonitor != null) pauseMonitor.stop();
    if (connectionCache != null) connectionCache.shutdown();
  }

  boolean supportsProxyuser() {
    return conf.getBoolean(HBASE_REST_SUPPORT_PROXYUSER, false);
  }

  private void registerCustomFilter(Configuration conf) {
    String[] filterList = conf.getStrings(Constants.CUSTOM_FILTERS);
    if (filterList != null) {
      for (String filterClass : filterList) {
        String[] filterPart = filterClass.split(":");
        if (filterPart.length != 2) {
          LOG.warn(
            "Invalid filter specification " + filterClass + " - skipping");
        } else {
          ParseFilter.registerFilter(filterPart[0], filterPart[1]);
        }
      }
    }
  }
}

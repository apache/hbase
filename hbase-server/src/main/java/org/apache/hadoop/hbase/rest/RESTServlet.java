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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
@InterfaceAudience.Private
public class RESTServlet implements Constants {
  private static Logger LOG = Logger.getLogger(RESTServlet.class);
  private static RESTServlet INSTANCE;
  private final Configuration conf;
  private final MetricsREST metrics = new MetricsREST();
  private final Map<String, ConnectionInfo>
    connections = new ConcurrentHashMap<String, ConnectionInfo>();
  private final KeyLocker<String> locker = new KeyLocker<String>();
  private final UserGroupInformation realUser;

  static final String CLEANUP_INTERVAL = "hbase.rest.connection.cleanup-interval";
  static final String MAX_IDLETIME = "hbase.rest.connection.max-idletime";

  static final String NULL_USERNAME = "--NULL--";

  private final ThreadLocal<String> effectiveUser = new ThreadLocal<String>() {
    protected String initialValue() {
      return NULL_USERNAME;
    }
  };

  // A chore to clean up idle connections.
  private final Chore connectionCleaner;
  private final Stoppable stoppable;

  class ConnectionInfo {
    final HConnection connection;
    final String userName;

    volatile HBaseAdmin admin;
    private long lastAccessTime;
    private boolean closed;

    ConnectionInfo(HConnection conn, String user) {
      lastAccessTime = EnvironmentEdgeManager.currentTimeMillis();
      connection = conn;
      closed = false;
      userName = user;
    }

    synchronized boolean updateAccessTime() {
      if (closed) {
        return false;
      }
      if (connection.isAborted() || connection.isClosed()) {
        LOG.info("Unexpected: cached HConnection is aborted/closed, removed from cache");
        connections.remove(userName);
        return false;
      }
      lastAccessTime = EnvironmentEdgeManager.currentTimeMillis();
      return true;
    }

    synchronized boolean timedOut(int maxIdleTime) {
      long timeoutTime = lastAccessTime + maxIdleTime;
      if (EnvironmentEdgeManager.currentTimeMillis() > timeoutTime) {
        connections.remove(userName);
        closed = true;
      }
      return false;
    }
  }

  class ConnectionCleaner extends Chore {
    private final int maxIdleTime;

    public ConnectionCleaner(int cleanInterval, int maxIdleTime) {
      super("REST-ConnectionCleaner", cleanInterval, stoppable);
      this.maxIdleTime = maxIdleTime;
    }

    @Override
    protected void chore() {
      for (Map.Entry<String, ConnectionInfo> entry: connections.entrySet()) {
        ConnectionInfo connInfo = entry.getValue();
        if (connInfo.timedOut(maxIdleTime)) {
          if (connInfo.admin != null) {
            try {
              connInfo.admin.close();
            } catch (Throwable t) {
              LOG.info("Got exception in closing idle admin", t);
            }
          }
          try {
            connInfo.connection.close();
          } catch (Throwable t) {
            LOG.info("Got exception in closing idle connection", t);
          }
        }
      }
    }
  }

  /**
   * @return the RESTServlet singleton instance
   */
  public synchronized static RESTServlet getInstance() {
    assert(INSTANCE != null);
    return INSTANCE;
  }

  /**
   * @param conf Existing configuration to use in rest servlet
   * @param realUser the login user
   * @return the RESTServlet singleton instance
   */
  public synchronized static RESTServlet getInstance(Configuration conf,
      UserGroupInformation realUser) {
    if (INSTANCE == null) {
      INSTANCE = new RESTServlet(conf, realUser);
    }
    return INSTANCE;
  }

  public synchronized static void stop() {
    if (INSTANCE != null)  INSTANCE = null;
  }

  /**
   * Constructor with existing configuration
   * @param conf existing configuration
   * @param realUser the login user
   */
  RESTServlet(final Configuration conf,
      final UserGroupInformation realUser) {
    stoppable = new Stoppable() {
      private volatile boolean isStopped = false;
      @Override public void stop(String why) { isStopped = true;}
      @Override public boolean isStopped() {return isStopped;}
    };

    int cleanInterval = conf.getInt(CLEANUP_INTERVAL, 10 * 1000);
    int maxIdleTime = conf.getInt(MAX_IDLETIME, 10 * 60 * 1000);
    connectionCleaner = new ConnectionCleaner(cleanInterval, maxIdleTime);
    Threads.setDaemonThreadRunning(connectionCleaner.getThread());

    this.realUser = realUser;
    this.conf = conf;
  }

  /**
   * Caller doesn't close the admin afterwards.
   * We need to manage it and close it properly.
   */
  HBaseAdmin getAdmin() throws IOException {
    ConnectionInfo connInfo = getCurrentConnection();
    if (connInfo.admin == null) {
      Lock lock = locker.acquireLock(effectiveUser.get());
      try {
        if (connInfo.admin == null) {
          connInfo.admin = new HBaseAdmin(connInfo.connection);
        }
      } finally {
        lock.unlock();
      }
    }
    return connInfo.admin;
  }

  /**
   * Caller closes the table afterwards.
   */
  HTableInterface getTable(String tableName) throws IOException {
    ConnectionInfo connInfo = getCurrentConnection();
    return connInfo.connection.getTable(tableName);
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
    this.effectiveUser.set(effectiveUser);
  }

  private ConnectionInfo getCurrentConnection() throws IOException {
    String userName = effectiveUser.get();
    ConnectionInfo connInfo = connections.get(userName);
    if (connInfo == null || !connInfo.updateAccessTime()) {
      Lock lock = locker.acquireLock(userName);
      try {
        connInfo = connections.get(userName);
        if (connInfo == null) {
          UserGroupInformation ugi = realUser;
          if (!userName.equals(NULL_USERNAME)) {
            ugi = UserGroupInformation.createProxyUser(userName, realUser);
          }
          User user = User.create(ugi);
          HConnection conn = HConnectionManager.createConnection(conf, user);
          connInfo = new ConnectionInfo(conn, userName);
          connections.put(userName, connInfo);
        }
      } finally {
        lock.unlock();
      }
    }
    return connInfo;
  }
}

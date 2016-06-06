/**
 *
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.commons.logging.LogFactory;

/**
 * A utility to store user specific HConnections in memory.
 * There is a chore to clean up connections idle for too long.
 * This class is used by REST server and Thrift server to
 * support authentication and impersonation.
 */
@InterfaceAudience.Private
public class ConnectionCache {
  private static final Log LOG = LogFactory.getLog(ConnectionCache.class);

  private final Map<String, ConnectionInfo>
   connections = new ConcurrentHashMap<String, ConnectionInfo>();
  private final KeyLocker<String> locker = new KeyLocker<String>();
  private final String realUserName;
  private final UserGroupInformation realUser;
  private final UserProvider userProvider;
  private final Configuration conf;

  private final ThreadLocal<String> effectiveUserNames =
      new ThreadLocal<String>() {
    @Override
    protected String initialValue() {
      return realUserName;
    }
  };

  public ConnectionCache(final Configuration conf,
      final UserProvider userProvider,
      final int cleanInterval, final int maxIdleTime) throws IOException {
    Stoppable stoppable = new Stoppable() {
      private volatile boolean isStopped = false;
      @Override public void stop(String why) { isStopped = true;}
      @Override public boolean isStopped() {return isStopped;}
    };

    Chore cleaner = new Chore("ConnectionCleaner", cleanInterval, stoppable) {
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
    };
    // Start the daemon cleaner chore
    Threads.setDaemonThreadRunning(cleaner.getThread());
    this.realUser = userProvider.getCurrent().getUGI();
    this.realUserName = realUser.getShortUserName();
    this.userProvider = userProvider;
    this.conf = conf;
  }

  /**
   * Set the current thread local effective user
   */
  public void setEffectiveUser(String user) {
    effectiveUserNames.set(user);
  }

  /**
   * Get the current thread local effective user
   */
  public String getEffectiveUser() {
    return effectiveUserNames.get();
  }

  /**
   * Caller doesn't close the admin afterwards.
   * We need to manage it and close it properly.
   */
  @SuppressWarnings("deprecation")
  public HBaseAdmin getAdmin() throws IOException {
    ConnectionInfo connInfo = getCurrentConnection();
    if (connInfo.admin == null) {
      Lock lock = locker.acquireLock(getEffectiveUser());
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
  public HTableInterface getTable(String tableName) throws IOException {
    ConnectionInfo connInfo = getCurrentConnection();
    return connInfo.connection.getTable(tableName);
  }

  /**
   * Get the cached connection for the current user.
   * If none or timed out, create a new one.
   */
  ConnectionInfo getCurrentConnection() throws IOException {
    String userName = getEffectiveUser();
    ConnectionInfo connInfo = connections.get(userName);
    if (connInfo == null || !connInfo.updateAccessTime()) {
      Lock lock = locker.acquireLock(userName);
      try {
        connInfo = connections.get(userName);
        if (connInfo == null) {
          UserGroupInformation ugi = realUser;
          if (!userName.equals(realUserName)) {
            ugi = UserGroupInformation.createProxyUser(userName, realUser);
          }
          User user = userProvider.create(ugi);
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
        return true;
      }
      return false;
    }
  }
}

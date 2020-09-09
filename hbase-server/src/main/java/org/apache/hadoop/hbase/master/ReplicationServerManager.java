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
package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ServerManager class manages info about replication servers.
 * <p>
 * Maintains lists of online and dead servers.
 * <p>
 * Servers are distinguished in two different ways.  A given server has a
 * location, specified by hostname and port, and of which there can only be one
 * online at any given time.  A server instance is specified by the location
 * (hostname and port) as well as the startcode (timestamp from when the server
 * was started).  This is used to differentiate a restarted instance of a given
 * server from the original instance.
 */
@InterfaceAudience.Private
public class ReplicationServerManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationServerManager.class);

  public static final String ONLINE_SERVER_REFRESH_INTERVAL =
      "hbase.master.replication.server.refresh.interval";
  public static final int ONLINE_SERVER_REFRESH_INTERVAL_DEFAULT = 60 * 1000; // 1 mins

  private final MasterServices master;

  /** Map of registered servers to their current load */
  private final ConcurrentNavigableMap<ServerName, ServerMetrics> onlineServers =
    new ConcurrentSkipListMap<>();

  private OnlineServerRefresher onlineServerRefresher;
  private int refreshPeriod;

  /**
   * Constructor.
   */
  public ReplicationServerManager(final MasterServices master) {
    this.master = master;
  }

  /**
   * start chore in ServerManager
   */
  public void startChore() {
    Configuration conf = master.getConfiguration();
    refreshPeriod = conf.getInt(ONLINE_SERVER_REFRESH_INTERVAL,
        ONLINE_SERVER_REFRESH_INTERVAL_DEFAULT);
    onlineServerRefresher = new OnlineServerRefresher("ReplicationServerRefresher", refreshPeriod);
    master.getChoreService().scheduleChore(onlineServerRefresher);
  }

  /**
   * Stop the ServerManager.
   */
  public void stop() {
    if (onlineServerRefresher != null) {
      onlineServerRefresher.cancel();
    }
  }

  public void serverReport(ServerName sn, ServerMetrics sl) {
    if (null == this.onlineServers.replace(sn, sl)) {
      if (!checkAndRecordNewServer(sn, sl)) {
        LOG.info("ReplicationServerReport ignored, could not record the server: {}", sn);
      }
    }
  }

  /**
   * Check is a server of same host and port already exists,
   * if not, or the existed one got a smaller start code, record it.
   *
   * @param serverName the server to check and record
   * @param sl the server load on the server
   * @return true if the server is recorded, otherwise, false
   */
  boolean checkAndRecordNewServer(final ServerName serverName, final ServerMetrics sl) {
    ServerName existingServer = null;
    synchronized (this.onlineServers) {
      existingServer = findServerWithSameHostnamePortWithLock(serverName);
      if (existingServer != null && (existingServer.getStartcode() > serverName.getStartcode())) {
        LOG.info("ReplicationServer serverName={} rejected; we already have {} registered with "
          + "same hostname and port", serverName, existingServer);
        return false;
      }
      recordNewServerWithLock(serverName, sl);
      // Note that we assume that same ts means same server, and don't expire in that case.
      if (existingServer != null && (existingServer.getStartcode() < serverName.getStartcode())) {
        LOG.info("Triggering server recovery; existingServer {} looks stale, new server: {}",
            existingServer, serverName);
        expireServerWithLock(existingServer);
      }
    }
    return true;
  }

  /**
   * Assumes onlineServers is locked.
   * @return ServerName with matching hostname and port.
   */
  private ServerName findServerWithSameHostnamePortWithLock(final ServerName serverName) {
    ServerName end = ServerName.valueOf(serverName.getHostname(), serverName.getPort(),
      Long.MAX_VALUE);

    ServerName r = onlineServers.lowerKey(end);
    if (r != null && ServerName.isSameAddress(r, serverName)) {
      return r;
    }
    return null;
  }

  private void recordNewServerWithLock(final ServerName serverName, final ServerMetrics sl) {
    LOG.info("Registering ReplicationServer={}", serverName);
    this.onlineServers.put(serverName, sl);
  }

  /**
   * Expire the passed server. Remove it from list of online servers
   */
  public void expireServerWithLock(final ServerName serverName) {
    onlineServers.remove(serverName);
  }

  /**
   * @return Read-only map of servers to serverinfo
   */
  public Map<ServerName, ServerMetrics> getOnlineServers() {
    // Presumption is that iterating the returned Map is OK.
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  /**
   * @return A copy of the internal list of online servers.
   */
  public List<ServerName> getOnlineServersList() {
    return new ArrayList<>(this.onlineServers.keySet());
  }

  /**
   * @param serverName server name
   * @return ServerMetrics if serverName is known else null
   */
  public ServerMetrics getServerMetrics(final ServerName serverName) {
    return this.onlineServers.get(serverName);
  }

  private class OnlineServerRefresher extends ScheduledChore {

    public OnlineServerRefresher(String name, int p) {
      super(name, master, p, 60 * 1000); // delay one minute before first execute
    }

    @Override
    protected void chore() {
      synchronized (onlineServers) {
        List<ServerName> servers = getOnlineServersList();
        servers.forEach(s -> {
          ServerMetrics metrics = onlineServers.get(s);
          if (metrics.getReportTimestamp() + refreshPeriod < System.currentTimeMillis()) {
            expireServerWithLock(s);
          }
        });
      }
    }
  }
}

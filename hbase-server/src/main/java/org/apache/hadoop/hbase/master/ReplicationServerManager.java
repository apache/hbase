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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerProtos;

/**
 * The ReplicationServerManager class manages info about replication servers.
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

  public static final String REPLICATION_SERVER_REFRESH_PERIOD =
    "hbase.replication.server.refresh.period";
  public static final int REPLICATION_SERVER_REFRESH_PERIOD_DEFAULT = 60 * 1000; // 1 mins

  private final MasterServices master;

  /**
   * Map of registered servers to their current load
   */
  private final ConcurrentNavigableMap<ServerName, Pair<ServerMetrics, Set<String>>> onlineServers =
    new ConcurrentSkipListMap<>();

  private ReplicationServerRefresher refresher;
  private int refreshPeriod;

  private ZKReplicationQueueStorage zkQueueStorage;

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
    this.zkQueueStorage = (ZKReplicationQueueStorage) ReplicationStorageFactory
      .getReplicationQueueStorage(master.getZooKeeper(), conf);
    refreshPeriod = conf.getInt(REPLICATION_SERVER_REFRESH_PERIOD,
      REPLICATION_SERVER_REFRESH_PERIOD_DEFAULT);
    refresher = new ReplicationServerRefresher("ReplicationServerRefresher", refreshPeriod);
    master.getChoreService().scheduleChore(refresher);
  }

  /**
   * Stop the ServerManager.
   */
  public void stop() {
    if (refresher != null) {
      refresher.cancel();
    }
  }

  public void serverReport(ServerName sn, ServerMetrics sm, Set<String> queueNodes) {
    if (!onlineServers.containsKey(sn)) {
      tryRecordNewServer(sn, sm, queueNodes);
    } else {
      onlineServers.put(sn, new Pair<>(sm, queueNodes));
    }
  }

  /**
   * Check is a server of same host and port already exists,
   * if not, or the existed one got a smaller start code, record it.
   */
  private void tryRecordNewServer(ServerName sn, ServerMetrics sm, Set<String> queueNodes) {
    ServerName existingServer = null;
    synchronized (this.onlineServers) {
      existingServer = findServerWithSameHostnamePort(sn);
      if (existingServer != null && (existingServer.getStartcode() > sn.getStartcode())) {
        LOG.info(
          "ReplicationServer serverName={} report rejected; we already have {} registered with "
            + "same hostname and port", sn, existingServer);
        return;
      }
      LOG.info("Registering ReplicationServer={} assigned replication queues: {}", sn,
        String.join(",", queueNodes));
      this.onlineServers.put(sn, new Pair<>(sm, queueNodes));
      // Note that we assume that same ts means same server, and don't expire in that case.
      if (existingServer != null && (existingServer.getStartcode() < sn.getStartcode())) {
        LOG.info("Triggering server recovery; existingServer {} looks stale, new server: {}",
          existingServer, sn);
        expireServer(existingServer);
      }
    }
  }

  /**
   * Assumes onlineServers is locked.
   * @return ServerName with matching hostname and port.
   */
  private ServerName findServerWithSameHostnamePort(final ServerName serverName) {
    ServerName end = ServerName.valueOf(serverName.getHostname(), serverName.getPort(),
      Long.MAX_VALUE);
    ServerName r = onlineServers.lowerKey(end);
    if (r != null && ServerName.isSameAddress(r, serverName)) {
      return r;
    }
    return null;
  }

  /**
   * Assumes onlineServers is locked.
   * Expire the passed server. Remove it from list of online servers
   */
  public void expireServer(final ServerName serverName) {
    LOG.info("Expiring ReplicationServer={}", serverName);
    onlineServers.remove(serverName);
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
    if (!this.onlineServers.containsKey(serverName)) {
      return null;
    }
    return this.onlineServers.get(serverName).getFirst();
  }

  /**
   * This chore is responsible for 3 things:
   * 1. Find all alive replication servers.
   * 2. Find all replication queues.
   * 3. Assign different queue to different replication server.
   */
  private class ReplicationServerRefresher extends ScheduledChore {

    public ReplicationServerRefresher(String name, int p) {
      super(name, master, p, p);
    }

    @Override
    protected void chore() {
      // Find all alive replication servers
      synchronized (onlineServers) {
        List<ServerName> servers = getOnlineServersList();
        servers.forEach(s -> {
          ServerMetrics metrics = onlineServers.get(s).getFirst();
          if (metrics.getReportTimestamp() + refreshPeriod < System.currentTimeMillis()) {
            expireServer(s);
          }
        });
      }
      Set<String> assignedQueueNodes =
        onlineServers.values().stream().map(Pair::getSecond).flatMap(Set::stream)
          .collect(Collectors.toSet());
      Map<ServerName, Set<String>> unassigned = new HashMap<>();
      // Because all replication queues is owned by region servers. List all region servers and get
      // their replication queues.
      for (ServerName producer : master.getServerManager().getOnlineServersList()) {
        try {
          List<String> queues = zkQueueStorage.getAllQueues(producer);
          for (String queue : queues) {
            String queueNode = zkQueueStorage.getQueueNode(producer, queue);
            LOG.debug("Found one replication queue {}", queueNode);
            if (!assignedQueueNodes.contains(queueNode)) {
              unassigned.computeIfAbsent(producer, p -> new HashSet<>()).add(queue);
            }
          }
        } catch (ReplicationException e) {
          LOG.warn("Failed to get all replication queues of server {}", producer, e);
        }
      }
      ServerName[] consumers = getOnlineServersList().stream().toArray(ServerName[]::new);
      if (consumers.length == 0) {
        LOG.warn("No replication server available!");
        return;
      }
      // Assign different queue to different replication server
      for (Map.Entry<ServerName, Set<String>> entry : unassigned.entrySet()) {
        ServerName producer = entry.getKey();
        for (String queueId : entry.getValue()) {
          ServerName consumer = consumers[ThreadLocalRandom.current().nextInt(consumers.length)];
          ReplicationServerProtos.StartReplicationSourceRequest request =
            ReplicationServerProtos.StartReplicationSourceRequest.newBuilder()
              .setServerName(ProtobufUtil.toServerName(producer)).setQueueId(queueId).build();
          try {
            FutureUtils.get(master.getAsyncClusterConnection().getReplicationServerAdmin(consumer)
              .startReplicationSource(request, 10000));
            LOG.warn("Started replication source on replication server {},"
              + " replication queue: producer={}, queueId={}", consumer, producer, queueId);
          } catch (IOException e) {
            // Just log the exception and the replication queue will be reassigned in next chore
            LOG.warn("Failed to start replication source on replication server {},"
              + " replication queue: producer={}, queueId={}", consumer, producer, queueId, e);
          }
        }
      }
    }
  }
}

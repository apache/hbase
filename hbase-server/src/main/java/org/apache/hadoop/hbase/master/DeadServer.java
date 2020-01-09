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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;


/**
 * Class to hold dead servers list and utility querying dead server list.
 * Servers are added when they expire or when we find them in filesystem on startup.
 * When a server crash procedure is queued, it will populate the processing list and
 * then remove the server from processing list when done. Servers are removed from
 * dead server list when a new instance is started over the old on same hostname and
 * port or when new Master comes online tidying up after all initialization. Processing
 * list and deadserver list are not tied together (you don't have to be in deadservers
 * list to be processing and vice versa).
 */
@InterfaceAudience.Private
public class DeadServer {
  private static final Logger LOG = LoggerFactory.getLogger(DeadServer.class);

  /**
   * Set of known dead servers.  On znode expiration, servers are added here.
   * This is needed in case of a network partitioning where the server's lease
   * expires, but the server is still running. After the network is healed,
   * and it's server logs are recovered, it will be told to call server startup
   * because by then, its regions have probably been reassigned.
   */
  private final Map<ServerName, Long> deadServers = new HashMap<>();

  /**
   * Set of dead servers currently being processed by a SCP.
   * Added to this list at the start of SCP and removed after it is done
   * processing the crash.
   */
  private final Set<ServerName> processingServers = new HashSet<>();

  /**
   * @param serverName server name.
   * @return true if this server is on the dead servers list false otherwise
   */
  public synchronized boolean isDeadServer(final ServerName serverName) {
    return deadServers.containsKey(serverName);
  }

  /**
   * Checks if there are currently any dead servers being processed by the
   * master.  Returns true if at least one region server is currently being
   * processed as dead.
   *
   * @return true if any RS are being processed as dead
   */
  synchronized boolean areDeadServersInProgress() {
    return !processingServers.isEmpty();
  }

  public synchronized Set<ServerName> copyServerNames() {
    Set<ServerName> clone = new HashSet<>(deadServers.size());
    clone.addAll(deadServers.keySet());
    return clone;
  }

  /**
   * Adds the server to the dead server list if it's not there already.
   */
  synchronized void putIfAbsent(ServerName sn) {
    this.deadServers.putIfAbsent(sn, EnvironmentEdgeManager.currentTime());
    processing(sn);
  }

  /**
   * Add <code>sn<</code> to set of processing deadservers.
   * @see #finish(ServerName)
   */
  public synchronized void processing(ServerName sn) {
    if (processingServers.add(sn)) {
      // Only log on add.
      LOG.debug("Processing {}; numProcessing={}", sn, processingServers.size());
    }
  }

  /**
   * Complete processing for this dead server.
   * @param sn ServerName for the dead server.
   * @see #processing(ServerName)
   */
  public synchronized void finish(ServerName sn) {
    if (processingServers.remove(sn)) {
      LOG.debug("Removed {} from processing; numProcessing={}", sn, processingServers.size());
    }
  }

  public synchronized int size() {
    return deadServers.size();
  }

  synchronized boolean isEmpty() {
    return deadServers.isEmpty();
  }

  /**
   * Handles restart of a server. The new server instance has a different start code.
   * The new start code should be greater than the old one. We don't check that here.
   * Removes the old server from deadserver list.
   *
   * @param newServerName Servername as either <code>host:port</code> or
   *                      <code>host,port,startcode</code>.
   * @return true if this server was dead before and coming back alive again
   */
  synchronized boolean cleanPreviousInstance(final ServerName newServerName) {
    Iterator<ServerName> it = deadServers.keySet().iterator();
    while (it.hasNext()) {
      if (cleanOldServerName(newServerName, it)) {
        return true;
      }
    }
    return false;
  }

  synchronized void cleanAllPreviousInstances(final ServerName newServerName) {
    Iterator<ServerName> it = deadServers.keySet().iterator();
    while (it.hasNext()) {
      cleanOldServerName(newServerName, it);
    }
  }

  /**
   * @param newServerName Server to match port and hostname against.
   * @param deadServerIterator Iterator primed so can call 'next' on it.
   * @return True if <code>newServerName</code> and current primed
   *   iterator ServerName have same host and port and we removed old server
   *   from iterator and from processing list.
   */
  private boolean cleanOldServerName(ServerName newServerName,
      Iterator<ServerName> deadServerIterator) {
    ServerName sn = deadServerIterator.next();
    if (ServerName.isSameAddress(sn, newServerName)) {
      // Remove from dead servers list. Don't remove from the processing list --
      // let the SCP do it when it is done.
      deadServerIterator.remove();
      return true;
    }
    return false;
  }

  @Override
  public synchronized String toString() {
    // Display unified set of servers from both maps
    Set<ServerName> servers = new HashSet<>();
    servers.addAll(deadServers.keySet());
    servers.addAll(processingServers);
    StringBuilder sb = new StringBuilder();
    for (ServerName sn : servers) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(sn.toString());
      // Star entries that are being processed
      if (processingServers.contains(sn)) {
        sb.append("*");
      }
    }
    return sb.toString();
  }

  /**
   * Extract all the servers dead since a given time, and sort them.
   * @param ts the time, 0 for all
   * @return a sorted array list, by death time, lowest values first.
   */
  synchronized List<Pair<ServerName, Long>> copyDeadServersSince(long ts) {
    List<Pair<ServerName, Long>> res =  new ArrayList<>(size());

    for (Map.Entry<ServerName, Long> entry:deadServers.entrySet()){
      if (entry.getValue() >= ts){
        res.add(new Pair<>(entry.getKey(), entry.getValue()));
      }
    }

    Collections.sort(res, (o1, o2) -> o1.getSecond().compareTo(o2.getSecond()));
    return res;
  }
  
  /**
   * Get the time when a server died
   * @param deadServerName the dead server name
   * @return the date when the server died 
   */
  public synchronized Date getTimeOfDeath(final ServerName deadServerName){
    Long time = deadServers.get(deadServerName);
    return time == null ? null : new Date(time);
  }

  /**
   * Called from rpc by operator cleaning up deadserver list.
   * @param deadServerName the dead server name
   * @return true if this server was removed
   */
  public synchronized boolean removeDeadServer(final ServerName deadServerName) {
    Preconditions.checkState(!processingServers.contains(deadServerName),
      "Asked to remove server still in processingServers set " + deadServerName +
          " (numProcessing=" + processingServers.size() + ")");
    return this.deadServers.remove(deadServerName) != null;
  }
}

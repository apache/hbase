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
package org.apache.hadoop.hbase.master;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Class to hold dead servers list and utility querying dead server list.
 * On znode expiration, servers are added here.
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
   * Set of dead servers currently being processed
   */
  private final Set<ServerName> processingServers = new HashSet<ServerName>();

  /**
   * A dead server that comes back alive has a different start code. The new start code should be
   *  greater than the old one, but we don't take this into account in this method.
   *
   * @param newServerName Servername as either <code>host:port</code> or
   *                      <code>host,port,startcode</code>.
   * @return true if this server was dead before and coming back alive again
   */
  public synchronized boolean cleanPreviousInstance(final ServerName newServerName) {
    Iterator<ServerName> it = deadServers.keySet().iterator();
    while (it.hasNext()) {
      ServerName sn = it.next();
      if (ServerName.isSameAddress(sn, newServerName)) {
        // remove from deadServers
        it.remove();
        // remove from processingServers
        boolean removed = processingServers.remove(sn);
        if (removed) {
          LOG.debug("Removed " + sn + " ; numProcessing=" + processingServers.size());
        }
        return true;
      }
    }

    return false;
  }

  /**
   * @param serverName server name.
   * @return true if this server is on the dead servers list false otherwise
   */
  public synchronized boolean isDeadServer(final ServerName serverName) {
    return deadServers.containsKey(serverName);
  }

  /**
   * @param serverName server name.
   * @return true if this server is on the processing servers list false otherwise
   */
  public synchronized boolean isProcessingServer(final ServerName serverName) {
    return processingServers.contains(serverName);
  }

  /**
   * Checks if there are currently any dead servers being processed by the
   * master.  Returns true if at least one region server is currently being
   * processed as dead.
   *
   * @return true if any RS are being processed as dead
   */
  public synchronized boolean areDeadServersInProgress() {
    return !processingServers.isEmpty();
  }

  public synchronized Set<ServerName> copyServerNames() {
    Set<ServerName> clone = new HashSet<>(deadServers.size());
    clone.addAll(deadServers.keySet());
    return clone;
  }


  /**
   * Adds the server to the dead server list if it's not there already.
   * @param sn the server name
   */
  public synchronized void add(ServerName sn) {
    add(sn, true);
  }

  /**
   * Adds the server to the dead server list if it's not there already.
   * @param sn the server name
   * @param processing whether there is an active SCP associated with the server
   */
  public synchronized void add(ServerName sn, boolean processing) {
    if (!deadServers.containsKey(sn)){
      deadServers.put(sn, EnvironmentEdgeManager.currentTime());
    }
    if (processing && processingServers.add(sn)) {
      LOG.debug("Added {}; numProcessing={}", sn, processingServers.size());
    }
  }

  /**
   * Notify that we started processing this dead server.
   * @param sn ServerName for the dead server.
   */
  public synchronized void notifyServer(ServerName sn) {
    boolean added = processingServers.add(sn);
    if (LOG.isDebugEnabled()) {
      if (added) {
        LOG.debug("Added " + sn + "; numProcessing=" + processingServers.size());
      }
      LOG.debug("Started processing " + sn + "; numProcessing=" + processingServers.size());
    }
  }

  /**
   * Complete processing for this dead server.
   * @param sn ServerName for the dead server.
   */
  public synchronized void finish(ServerName sn) {
    boolean removed = processingServers.remove(sn);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished processing " + sn + "; numProcessing=" + processingServers.size());
      if (removed) {
        LOG.debug("Removed " + sn + " ; numProcessing=" + processingServers.size());
      }
    }
  }

  public synchronized int size() {
    return deadServers.size();
  }

  public synchronized boolean isEmpty() {
    return deadServers.isEmpty();
  }

  public synchronized void cleanAllPreviousInstances(final ServerName newServerName) {
    Iterator<ServerName> it = deadServers.keySet().iterator();
    while (it.hasNext()) {
      ServerName sn = it.next();
      if (ServerName.isSameAddress(sn, newServerName)) {
        // remove from deadServers
        it.remove();
        // remove from processingServers
        boolean removed = processingServers.remove(sn);
        if (removed) {
          LOG.debug("Removed " + sn + " ; numProcessing=" + processingServers.size());
        }
      }
    }
  }

  @Override
  public synchronized String toString() {
    // Display unified set of servers from both maps
    Set<ServerName> servers = new HashSet<ServerName>();
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
  public synchronized List<Pair<ServerName, Long>> copyDeadServersSince(long ts){
    List<Pair<ServerName, Long>> res =  new ArrayList<>(size());

    for (Map.Entry<ServerName, Long> entry:deadServers.entrySet()){
      if (entry.getValue() >= ts){
        res.add(new Pair<>(entry.getKey(), entry.getValue()));
      }
    }

    Collections.sort(res, ServerNameDeathDateComparator);
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

  private static Comparator<Pair<ServerName, Long>> ServerNameDeathDateComparator =
      new Comparator<Pair<ServerName, Long>>(){

    @Override
    public int compare(Pair<ServerName, Long> o1, Pair<ServerName, Long> o2) {
      return o1.getSecond().compareTo(o2.getSecond());
    }
  };

  /**
   * remove the specified dead server
   * @param deadServerName the dead server name
   * @return true if this server was removed
   */

  public synchronized boolean removeDeadServer(final ServerName deadServerName) {
    Preconditions.checkState(!processingServers.contains(deadServerName),
      "Asked to remove server still in processingServers set " + deadServerName +
          " (numProcessing=" + processingServers.size() + ")");
    if (deadServers.remove(deadServerName) == null) {
      return false;
    }
    return true;
  }
}

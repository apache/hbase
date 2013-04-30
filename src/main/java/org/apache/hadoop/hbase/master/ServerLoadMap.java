/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mortbay.log.Log;

import com.google.common.collect.TreeMultimap;

/** Maintains a map from regionservers name to server load and vice versa */
public class ServerLoadMap<L extends Comparable<?>> {

  /** SortedMap server load -> Set of server names */
  private final TreeMultimap<L, String> loadToServers = TreeMultimap.create();

  /** Map of server names -> server load */
  private final Map<String, L> serversToLoad = new HashMap<String, L>();

  private ReadWriteLock rwLock = new ReentrantReadWriteLock();

  public void updateServerLoad(String serverName, L load) {

    rwLock.writeLock().lock();

    try {
      L currentLoad = serversToLoad.get(serverName);

      if (currentLoad != null) {
        // Remove the server from its current load bucket.
        loadToServers.remove(currentLoad, serverName);
      }

      if (load != null) {
        // Set the new load for the server and add it to its new load bucket.
        serversToLoad.put(serverName, load);
        loadToServers.put(load, serverName);
      } else {
        // Remove server -> load mapping.
        serversToLoad.remove(serverName);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public void removeServerLoad(String serverName) {
    rwLock.writeLock().lock();
    try {
      L load = serversToLoad.remove(serverName);
      if (load != null) {
        loadToServers.remove(load, serverName);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public L get(String serverName) {
    rwLock.readLock().lock();
    try {
      return serversToLoad.get(serverName);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /** Takes a snapshot of all entries in the server to load map */
  public List<Map.Entry<String, L>> entries() {
    rwLock.readLock().lock();
    try {
      return new ArrayList<Map.Entry<String, L>>(serversToLoad.entrySet());
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public SortedMap<L, Collection<String>> getLightServers(L referenceLoad) {
    rwLock.readLock().lock();
    try {
      SortedMap<L, Collection<String>> lightServers =
          new TreeMap<L, Collection<String>>();
      lightServers.putAll(loadToServers.asMap().headMap(referenceLoad));
      removeBlacklistedServers(lightServers);
      return lightServers;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public SortedMap<L, Collection<String>> getHeavyServers(L referenceLoad) {
    rwLock.readLock().lock();
    try {
      SortedMap<L, Collection<String>> heavyServers =
          new TreeMap<L, Collection<String>>();
      heavyServers.putAll(loadToServers.asMap().tailMap(referenceLoad));
      removeBlacklistedServers(heavyServers);
      return heavyServers;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private void removeBlacklistedServers(SortedMap<L, Collection<String>> map) {

    if (!ServerManager.hasBlacklistedServers()) {
      return;
    }

    // Iterate through all the blacklisted Servers and remove them from the map
    for (String server : ServerManager.getBlacklistedServers()) {
      // Get the load of the blacklisted server
      L serverToLoad = serversToLoad.get(server);

      // remove the entry from the collection of the current map
      if (serverToLoad != null) {
        map.get(serverToLoad).remove(server);
      }
    }
  }

  public int size() {
    rwLock.readLock().lock();
    try {
      return serversToLoad.size();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public boolean isMostLoadedServer(String serverName) {
    rwLock.readLock().lock();
    try {
      L maxLoad = loadToServers.asMap().lastKey();
      if (maxLoad == null) {
        return false;
      }
      return loadToServers.get(maxLoad).contains(serverName);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public L getLowestLoad() {
    rwLock.readLock().lock();
    try {
      return loadToServers.asMap().firstKey();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public int numServersByLoad(L load) {
    rwLock.readLock().lock();
    try {
      Set<String> servers = loadToServers.get(load);
      if (servers == null) {
        return 0;
      }
      return servers.size();
    } finally {
      rwLock.readLock().unlock();
    }
  }

}

/**
 * Copyright 2011 The Apache Software Foundation
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;

public class AssignmentDomain {
  protected static final Log LOG =
    LogFactory.getLog(AssignmentDomain.class.getClass());
  private Map<String, List<HServerAddress>> rackToRegionServerMap;
  private List<String> uniqueRackList;
  private RackManager rackManager;
  private Map<HServerAddress, String> regionServerToRackMap;
  private Random random;
  
  public AssignmentDomain(Configuration conf) {
    rackToRegionServerMap = new HashMap<String, List<HServerAddress>>();
    regionServerToRackMap = new HashMap<HServerAddress, String>();
    uniqueRackList = new ArrayList<String>();
    rackManager = new RackManager(conf);
    random = new Random();
  }

  public AssignmentDomain(Configuration conf, HTableDescriptor htd,
      Collection<HServerAddress> liveServers) {
    this(conf);

    List<HServerAddress> servers = null;
    // If the table is pinned to servers then respect that.
    if (htd.getServers() != null) {
      Set<HServerAddress> hServerAddresses = new HashSet<>(htd.getServers());
      hServerAddresses.retainAll(liveServers);
      servers = Lists.newArrayList(hServerAddresses);
    } else {
      // Otherwise use all of the live servers
      servers = Lists.newArrayList(liveServers);
    }

    // Shuffle the server list based on the tableName
    Random random = new Random(htd.getNameAsString().hashCode());
    Collections.shuffle(servers, random);
    this.addServers(servers);
  }
  
  /**
   * Set the random seed
   * @param seed
   */
  public void setRandomSeed(long seed) {
    random.setSeed(seed);
  }

  /**
   * Get the rack name in this domain for the server.
   * @param server
   * @return
   */
  public String getRack(HServerAddress server) {
    if (server == null)
      return null;
    return regionServerToRackMap.get(server);
  }

  /**
   * Get a random rack except for the current rack
   * @param skipRackSet
   * @return the random rack except for any Rack from the skipRackSet
   * @throws IOException 
   */
  public String getOneRandomRack(Set<String> skipRackSet) throws IOException {
    if (skipRackSet == null || uniqueRackList.size() <= skipRackSet.size()) {
      throw new IOException("Cannot randomly pick another random server");
    }

    String randomRack;
    do {
      int randomIndex = random.nextInt(this.uniqueRackList.size());
      randomRack = this.uniqueRackList.get(randomIndex);
    } while (skipRackSet.contains(randomRack));
    
    return randomRack;
  }
  
  /**
   * Get one random server from the rack
   * @param rack
   * @return
   * @throws IOException
   */
  public HServerAddress getOneRandomServer(String rack) throws IOException {
    return this.getOneRandomServer(rack, null);
  }
  
  /**
   * Get a random server from the rack except for the servers in the skipServerSet
   * @param skipServerSet
   * @return the random server except for any servers from the skipServerSet
   * @throws IOException 
   */
  public HServerAddress getOneRandomServer(String rack,
      Set<HServerAddress> skipServerSet) throws IOException {
    if(rack == null) return null;
    List<HServerAddress> serverList = this.rackToRegionServerMap.get(rack);
    if (serverList == null) return null;
    
    // Get a random server except for any servers from the skip set
    if (skipServerSet != null && serverList.size() <= skipServerSet.size()) {
      throw new IOException("Cannot randomly pick another random server");
    }
    
    HServerAddress randomServer;
    do {
      int randomIndex = random.nextInt(serverList.size());
      randomServer = serverList.get(randomIndex);
    } while (skipServerSet != null && skipServerSet.contains(randomServer));
    
    return randomServer;
  }
  
  /**
   * @return the total number of unique rack in the domain.
   */
  public int getTotalRackNum() {
    return this.uniqueRackList.size();
  }
  
  /**
   * Get the list of region severs in the rack
   * @param rack
   * @return the list of region severs in the rack
   */
  public List<HServerAddress> getServersFromRack(String rack) {
    return this.rackToRegionServerMap.get(rack);
  }
  
  /**
   * Add a server to the assignment domain
   * @param server
   */
  public void addServer(HServerAddress server) {
    // For a new server
    String rackName = this.rackManager.getRack(server);
    List<HServerAddress> serverList = this.rackToRegionServerMap.get(rackName);
    if (serverList == null) {
      serverList = new ArrayList<HServerAddress>();
      // Add the current rack to the unique rack list
      this.uniqueRackList.add(rackName);
    }
    if (!serverList.contains(server)) {
      serverList.add(server);
      this.rackToRegionServerMap.put(rackName, serverList);
      this.regionServerToRackMap.put(server, rackName);
    }
  }

  /**
   * Add a list of servers to the assignment domain
   * @param servers
   */
  public void addServers(List<HServerAddress> servers) {
    for (HServerAddress server : servers) {
      this.addServer(server);
    }
  }
  
  public Set<HServerAddress> getAllServers() {
    return regionServerToRackMap.keySet();
  }
  
  /**
   * Get the region server to rack map
   */
  public Map<HServerAddress, String> getRegionServerToRackMap() {
    return this.regionServerToRackMap;
  }

  /**
   * Get the rack to region server map
   */
  public Map<String, List<HServerAddress>> getRackToRegionServerMap() {
    return this.rackToRegionServerMap;
  }
  
  /**
   * @return true if there is no rack in the assignment domain
   */
  public boolean isEmpty() {
    return uniqueRackList.isEmpty();
  }
  
  /**
   * @return true if can place the favored nodes
   */
  public boolean canPlaceFavoredNodes() {
    int serverSize = this.regionServerToRackMap.keySet().size();
    if (serverSize < HConstants.FAVORED_NODES_NUM)
      return false;
    return true;
  }
}

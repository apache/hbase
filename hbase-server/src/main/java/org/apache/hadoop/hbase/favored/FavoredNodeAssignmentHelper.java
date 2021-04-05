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

package org.apache.hadoop.hbase.favored;

import static org.apache.hadoop.hbase.ServerName.NON_STARTCODE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.FavoredNodes;

/**
 * Helper class for {@link FavoredNodeLoadBalancer} that has all the intelligence for racks,
 * meta scans, etc. Instantiated by the {@link FavoredNodeLoadBalancer} when needed (from
 * within calls like {@link FavoredNodeLoadBalancer#randomAssignment(RegionInfo, List)}).
 * All updates to favored nodes should only be done from {@link FavoredNodesManager} and not
 * through this helper class (except for tests).
 */
@InterfaceAudience.Private
public class FavoredNodeAssignmentHelper {
  private static final Logger LOG = LoggerFactory.getLogger(FavoredNodeAssignmentHelper.class);
  private RackManager rackManager;
  private Map<String, List<ServerName>> rackToRegionServerMap;
  private List<String> uniqueRackList;
  // This map serves as a cache for rack to sn lookups. The num of
  // region server entries might not match with that is in servers.
  private Map<String, String> regionServerToRackMap;
  private Random random;
  private List<ServerName> servers;
  public static final byte [] FAVOREDNODES_QUALIFIER = Bytes.toBytes("fn");
  public final static short FAVORED_NODES_NUM = 3;
  public final static short MAX_ATTEMPTS_FN_GENERATION = 10;

  public FavoredNodeAssignmentHelper(final List<ServerName> servers, Configuration conf) {
    this(servers, new RackManager(conf));
  }

  public FavoredNodeAssignmentHelper(final List<ServerName> servers,
      final RackManager rackManager) {
    this.servers = servers;
    this.rackManager = rackManager;
    this.rackToRegionServerMap = new HashMap<>();
    this.regionServerToRackMap = new HashMap<>();
    this.uniqueRackList = new ArrayList<>();
    this.random = new Random();
  }

  // Always initialize() when FavoredNodeAssignmentHelper is constructed.
  public void initialize() {
    for (ServerName sn : this.servers) {
      String rackName = getRackOfServer(sn);
      List<ServerName> serverList = this.rackToRegionServerMap.get(rackName);
      if (serverList == null) {
        serverList = Lists.newArrayList();
        // Add the current rack to the unique rack list
        this.uniqueRackList.add(rackName);
        this.rackToRegionServerMap.put(rackName, serverList);
      }
      for (ServerName serverName : serverList) {
        if (ServerName.isSameAddress(sn, serverName)) {
          // The server is already present, ignore.
          break;
        }
      }
      serverList.add((sn));
      this.regionServerToRackMap.put(sn.getHostname(), rackName);
    }
  }

  /**
   * Update meta table with favored nodes info
   * @param regionToFavoredNodes map of RegionInfo's to their favored nodes
   * @param connection connection to be used
   * @throws IOException
   */
  public static void updateMetaWithFavoredNodesInfo(
      Map<RegionInfo, List<ServerName>> regionToFavoredNodes,
      Connection connection) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (Map.Entry<RegionInfo, List<ServerName>> entry : regionToFavoredNodes.entrySet()) {
      Put put = makePutFromRegionInfo(entry.getKey(), entry.getValue());
      if (put != null) {
        puts.add(put);
      }
    }
    MetaTableAccessor.putsToMetaTable(connection, puts);
    LOG.info("Added " + puts.size() + " regions in META");
  }

  /**
   * Update meta table with favored nodes info
   * @param regionToFavoredNodes
   * @param conf
   * @throws IOException
   */
  public static void updateMetaWithFavoredNodesInfo(
      Map<RegionInfo, List<ServerName>> regionToFavoredNodes,
      Configuration conf) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (Map.Entry<RegionInfo, List<ServerName>> entry : regionToFavoredNodes.entrySet()) {
      Put put = makePutFromRegionInfo(entry.getKey(), entry.getValue());
      if (put != null) {
        puts.add(put);
      }
    }
    // Write the region assignments to the meta table.
    // TODO: See above overrides take a Connection rather than a Configuration only the
    // Connection is a short circuit connection. That is not going to good in all cases, when
    // master and meta are not colocated. Fix when this favored nodes feature is actually used
    // someday.
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table metaTable = connection.getTable(TableName.META_TABLE_NAME)) {
        metaTable.put(puts);
      }
    }
    LOG.info("Added " + puts.size() + " regions in META");
  }

  /**
   * Generates and returns a Put containing the region info for the catalog table and the servers
   * @return Put object
   */
  private static Put makePutFromRegionInfo(RegionInfo regionInfo, List<ServerName> favoredNodeList)
      throws IOException {
    Put put = null;
    if (favoredNodeList != null) {
      long time = EnvironmentEdgeManager.currentTime();
      put = MetaTableAccessor.makePutFromRegionInfo(regionInfo, time);
      byte[] favoredNodes = getFavoredNodes(favoredNodeList);
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(put.getRow())
          .setFamily(HConstants.CATALOG_FAMILY)
          .setQualifier(FAVOREDNODES_QUALIFIER)
          .setTimestamp(time)
          .setType(Type.Put)
          .setValue(favoredNodes)
          .build());
      LOG.debug("Create the region {} with favored nodes {}", regionInfo.getRegionNameAsString(),
        favoredNodeList);
    }
    return put;
  }

  /**
   * @param favoredNodes The PB'ed bytes of favored nodes
   * @return the array of {@link ServerName} for the byte array of favored nodes.
   * @throws IOException
   */
  public static ServerName[] getFavoredNodesList(byte[] favoredNodes) throws IOException {
    FavoredNodes f = FavoredNodes.parseFrom(favoredNodes);
    List<HBaseProtos.ServerName> protoNodes = f.getFavoredNodeList();
    ServerName[] servers = new ServerName[protoNodes.size()];
    int i = 0;
    for (HBaseProtos.ServerName node : protoNodes) {
      servers[i++] = ProtobufUtil.toServerName(node);
    }
    return servers;
  }

  /**
   * @param serverAddrList
   * @return PB'ed bytes of {@link FavoredNodes} generated by the server list.
   */
  public static byte[] getFavoredNodes(List<ServerName> serverAddrList) {
    FavoredNodes.Builder f = FavoredNodes.newBuilder();
    for (ServerName s : serverAddrList) {
      HBaseProtos.ServerName.Builder b = HBaseProtos.ServerName.newBuilder();
      b.setHostName(s.getHostname());
      b.setPort(s.getPort());
      b.setStartCode(ServerName.NON_STARTCODE);
      f.addFavoredNode(b.build());
    }
    return f.build().toByteArray();
  }

  // Place the regions round-robin across the racks picking one server from each
  // rack at a time. Start with a random rack, and a random server from every rack.
  // If a rack doesn't have enough servers it will go to the next rack and so on.
  // for choosing a primary.
  // For example, if 4 racks (r1 .. r4) with 8 servers (s1..s8) each, one possible
  // placement could be r2:s5, r3:s5, r4:s5, r1:s5, r2:s6, r3:s6..
  // If there were fewer servers in one rack, say r3, which had 3 servers, one possible
  // placement could be r2:s5, <skip-r3>, r4:s5, r1:s5, r2:s6, <skip-r3> ...
  // The regions should be distributed proportionately to the racksizes
  public void placePrimaryRSAsRoundRobin(Map<ServerName, List<RegionInfo>> assignmentMap,
      Map<RegionInfo, ServerName> primaryRSMap, List<RegionInfo> regions) {
    List<String> rackList = new ArrayList<>(rackToRegionServerMap.size());
    rackList.addAll(rackToRegionServerMap.keySet());
    int rackIndex = random.nextInt(rackList.size());
    int maxRackSize = 0;
    for (Map.Entry<String,List<ServerName>> r : rackToRegionServerMap.entrySet()) {
      if (r.getValue().size() > maxRackSize) {
        maxRackSize = r.getValue().size();
      }
    }
    int numIterations = 0;
    // Initialize the current processing host index.
    int serverIndex = random.nextInt(maxRackSize);
    for (RegionInfo regionInfo : regions) {
      List<ServerName> currentServerList;
      String rackName;
      while (true) {
        rackName = rackList.get(rackIndex);
        numIterations++;
        // Get the server list for the current rack
        currentServerList = rackToRegionServerMap.get(rackName);

        if (serverIndex >= currentServerList.size()) { //not enough machines in this rack
          if (numIterations % rackList.size() == 0) {
            if (++serverIndex >= maxRackSize) serverIndex = 0;
          }
          if ((++rackIndex) >= rackList.size()) {
            rackIndex = 0; // reset the rack index to 0
          }
        } else break;
      }

      // Get the current process region server
      ServerName currentServer = currentServerList.get(serverIndex);

      // Place the current region with the current primary region server
      primaryRSMap.put(regionInfo, currentServer);
      if (assignmentMap != null) {
        List<RegionInfo> regionsForServer = assignmentMap.get(currentServer);
        if (regionsForServer == null) {
          regionsForServer = new ArrayList<>();
          assignmentMap.put(currentServer, regionsForServer);
        }
        regionsForServer.add(regionInfo);
      }

      // Set the next processing index
      if (numIterations % rackList.size() == 0) {
        ++serverIndex;
      }
      if ((++rackIndex) >= rackList.size()) {
        rackIndex = 0; // reset the rack index to 0
      }
    }
  }

  public Map<RegionInfo, ServerName[]> placeSecondaryAndTertiaryRS(
      Map<RegionInfo, ServerName> primaryRSMap) {
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap = new HashMap<>();
    for (Map.Entry<RegionInfo, ServerName> entry : primaryRSMap.entrySet()) {
      // Get the target region and its primary region server rack
      RegionInfo regionInfo = entry.getKey();
      ServerName primaryRS = entry.getValue();
      try {
        // Create the secondary and tertiary region server pair object.
        ServerName[] favoredNodes = getSecondaryAndTertiary(regionInfo, primaryRS);
        if (favoredNodes != null) {
          secondaryAndTertiaryMap.put(regionInfo, favoredNodes);
          LOG.debug("Place the secondary and tertiary region server for region "
              + regionInfo.getRegionNameAsString());
        }
      } catch (Exception e) {
        LOG.warn("Cannot place the favored nodes for region " +
            regionInfo.getRegionNameAsString() + " because " + e, e);
        continue;
      }
    }
    return secondaryAndTertiaryMap;
  }

  public ServerName[] getSecondaryAndTertiary(RegionInfo regionInfo, ServerName primaryRS)
      throws IOException {

    ServerName[] favoredNodes;// Get the rack for the primary region server
    String primaryRack = getRackOfServer(primaryRS);

    if (getTotalNumberOfRacks() == 1) {
      favoredNodes = singleRackCase(regionInfo, primaryRS, primaryRack);
    } else {
      favoredNodes = multiRackCase(regionInfo, primaryRS, primaryRack);
    }
    return favoredNodes;
  }

  private Map<ServerName, Set<RegionInfo>> mapRSToPrimaries(
      Map<RegionInfo, ServerName> primaryRSMap) {
    Map<ServerName, Set<RegionInfo>> primaryServerMap = new HashMap<>();
    for (Entry<RegionInfo, ServerName> e : primaryRSMap.entrySet()) {
      Set<RegionInfo> currentSet = primaryServerMap.get(e.getValue());
      if (currentSet == null) {
        currentSet = new HashSet<>();
      }
      currentSet.add(e.getKey());
      primaryServerMap.put(e.getValue(), currentSet);
    }
    return primaryServerMap;
  }

  /**
   * For regions that share the primary, avoid placing the secondary and tertiary
   * on a same RS. Used for generating new assignments for the
   * primary/secondary/tertiary RegionServers
   * @param primaryRSMap
   * @return the map of regions to the servers the region-files should be hosted on
   */
  public Map<RegionInfo, ServerName[]> placeSecondaryAndTertiaryWithRestrictions(
      Map<RegionInfo, ServerName> primaryRSMap) {
    Map<ServerName, Set<RegionInfo>> serverToPrimaries =
        mapRSToPrimaries(primaryRSMap);
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap = new HashMap<>();

    for (Entry<RegionInfo, ServerName> entry : primaryRSMap.entrySet()) {
      // Get the target region and its primary region server rack
      RegionInfo regionInfo = entry.getKey();
      ServerName primaryRS = entry.getValue();
      try {
        // Get the rack for the primary region server
        String primaryRack = getRackOfServer(primaryRS);
        ServerName[] favoredNodes = null;
        if (getTotalNumberOfRacks() == 1) {
          // Single rack case: have to pick the secondary and tertiary
          // from the same rack
          favoredNodes = singleRackCase(regionInfo, primaryRS, primaryRack);
        } else {
          favoredNodes = multiRackCaseWithRestrictions(serverToPrimaries,
              secondaryAndTertiaryMap, primaryRack, primaryRS, regionInfo);
        }
        if (favoredNodes != null) {
          secondaryAndTertiaryMap.put(regionInfo, favoredNodes);
          LOG.debug("Place the secondary and tertiary region server for region "
              + regionInfo.getRegionNameAsString());
        }
      } catch (Exception e) {
        LOG.warn("Cannot place the favored nodes for region "
            + regionInfo.getRegionNameAsString() + " because " + e, e);
        continue;
      }
    }
    return secondaryAndTertiaryMap;
  }

  private ServerName[] multiRackCaseWithRestrictions(
      Map<ServerName, Set<RegionInfo>> serverToPrimaries,
      Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap,
      String primaryRack, ServerName primaryRS, RegionInfo regionInfo) throws IOException {
    // Random to choose the secondary and tertiary region server
    // from another rack to place the secondary and tertiary
    // Random to choose one rack except for the current rack
    Set<String> rackSkipSet = new HashSet<>();
    rackSkipSet.add(primaryRack);
    String secondaryRack = getOneRandomRack(rackSkipSet);
    List<ServerName> serverList = getServersFromRack(secondaryRack);
    Set<ServerName> serverSet = new HashSet<>(serverList);
    ServerName[] favoredNodes;
    if (serverList.size() >= 2) {
      // Randomly pick up two servers from this secondary rack
      // Skip the secondary for the tertiary placement
      // skip the servers which share the primary already
      Set<RegionInfo> primaries = serverToPrimaries.get(primaryRS);
      Set<ServerName> skipServerSet = new HashSet<>();
      while (true) {
        ServerName[] secondaryAndTertiary = null;
        if (primaries.size() > 1) {
          // check where his tertiary and secondary are
          for (RegionInfo primary : primaries) {
            secondaryAndTertiary = secondaryAndTertiaryMap.get(primary);
            if (secondaryAndTertiary != null) {
              if (getRackOfServer(secondaryAndTertiary[0]).equals(secondaryRack)) {
                skipServerSet.add(secondaryAndTertiary[0]);
              }
              if (getRackOfServer(secondaryAndTertiary[1]).equals(secondaryRack)) {
                skipServerSet.add(secondaryAndTertiary[1]);
              }
            }
          }
        }
        if (skipServerSet.size() + 2 <= serverSet.size())
          break;
        skipServerSet.clear();
        rackSkipSet.add(secondaryRack);
        // we used all racks
        if (rackSkipSet.size() == getTotalNumberOfRacks()) {
          // remove the last two added and break
          skipServerSet.remove(secondaryAndTertiary[0]);
          skipServerSet.remove(secondaryAndTertiary[1]);
          break;
        }
        secondaryRack = getOneRandomRack(rackSkipSet);
        serverList = getServersFromRack(secondaryRack);
        serverSet = new HashSet<>(serverList);
      }

      // Place the secondary RS
      ServerName secondaryRS = getOneRandomServer(secondaryRack, skipServerSet);
      skipServerSet.add(secondaryRS);
      // Place the tertiary RS
      ServerName tertiaryRS = getOneRandomServer(secondaryRack, skipServerSet);

      if (secondaryRS == null || tertiaryRS == null) {
        LOG.error("Cannot place the secondary and tertiary"
            + " region server for region "
            + regionInfo.getRegionNameAsString());
      }
      // Create the secondary and tertiary pair
      favoredNodes = new ServerName[2];
      favoredNodes[0] = secondaryRS;
      favoredNodes[1] = tertiaryRS;
    } else {
      // Pick the secondary rs from this secondary rack
      // and pick the tertiary from another random rack
      favoredNodes = new ServerName[2];
      ServerName secondary = getOneRandomServer(secondaryRack);
      favoredNodes[0] = secondary;

      // Pick the tertiary
      if (getTotalNumberOfRacks() == 2) {
        // Pick the tertiary from the same rack of the primary RS
        Set<ServerName> serverSkipSet = new HashSet<>();
        serverSkipSet.add(primaryRS);
        favoredNodes[1] = getOneRandomServer(primaryRack, serverSkipSet);
      } else {
        // Pick the tertiary from another rack
        rackSkipSet.add(secondaryRack);
        String tertiaryRandomRack = getOneRandomRack(rackSkipSet);
        favoredNodes[1] = getOneRandomServer(tertiaryRandomRack);
      }
    }
    return favoredNodes;
  }

  private ServerName[] singleRackCase(RegionInfo regionInfo,
      ServerName primaryRS,
      String primaryRack) throws IOException {
    // Single rack case: have to pick the secondary and tertiary
    // from the same rack
    List<ServerName> serverList = getServersFromRack(primaryRack);
    if ((serverList == null) || (serverList.size() <= 2)) {
      // Single region server case: cannot not place the favored nodes
      // on any server;
      return null;
    } else {
      // Randomly select two region servers from the server list and make sure
      // they are not overlap with the primary region server;
     Set<ServerName> serverSkipSet = new HashSet<>();
     serverSkipSet.add(primaryRS);

     // Place the secondary RS
     ServerName secondaryRS = getOneRandomServer(primaryRack, serverSkipSet);
     // Skip the secondary for the tertiary placement
     serverSkipSet.add(secondaryRS);
     ServerName tertiaryRS = getOneRandomServer(primaryRack, serverSkipSet);

     if (secondaryRS == null || tertiaryRS == null) {
       LOG.error("Cannot place the secondary, tertiary favored node for region " +
           regionInfo.getRegionNameAsString());
     }
     // Create the secondary and tertiary pair
     ServerName[] favoredNodes = new ServerName[2];
     favoredNodes[0] = secondaryRS;
     favoredNodes[1] = tertiaryRS;
     return favoredNodes;
    }
  }

  /**
   * Place secondary and tertiary nodes in a multi rack case.
   * If there are only two racks, then we try the place the secondary
   * and tertiary on different rack than primary. But if the other rack has
   * only one region server, then we place primary and tertiary on one rack
   * and secondary on another. The aim is two distribute the three favored nodes
   * on >= 2 racks.
   * TODO: see how we can use generateMissingFavoredNodeMultiRack API here
   * @param regionInfo Region for which we are trying to generate FN
   * @param primaryRS The primary favored node.
   * @param primaryRack The rack of the primary favored node.
   * @return Array containing secondary and tertiary favored nodes.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private ServerName[] multiRackCase(RegionInfo regionInfo, ServerName primaryRS,
      String primaryRack) throws IOException {

    List<ServerName>favoredNodes = Lists.newArrayList(primaryRS);
    // Create the secondary and tertiary pair
    ServerName secondaryRS = generateMissingFavoredNodeMultiRack(favoredNodes);
    favoredNodes.add(secondaryRS);
    String secondaryRack = getRackOfServer(secondaryRS);

    ServerName tertiaryRS;
    if (primaryRack.equals(secondaryRack)) {
      tertiaryRS = generateMissingFavoredNode(favoredNodes);
    } else {
      // Try to place tertiary in secondary RS rack else place on primary rack.
      tertiaryRS = getOneRandomServer(secondaryRack, Sets.newHashSet(secondaryRS));
      if (tertiaryRS == null) {
        tertiaryRS = getOneRandomServer(primaryRack, Sets.newHashSet(primaryRS));
      }
      // We couldn't find anything in secondary rack, get any FN
      if (tertiaryRS == null) {
        tertiaryRS = generateMissingFavoredNode(Lists.newArrayList(primaryRS, secondaryRS));
      }
    }
    return new ServerName[]{ secondaryRS, tertiaryRS };
  }

  public boolean canPlaceFavoredNodes() {
    return (this.servers.size() >= FAVORED_NODES_NUM);
  }

  private int getTotalNumberOfRacks() {
    return this.uniqueRackList.size();
  }

  private List<ServerName> getServersFromRack(String rack) {
    return this.rackToRegionServerMap.get(rack);
  }

  /**
   * Gets a random server from the specified rack and skips anything specified.

   * @param rack rack from a server is needed
   * @param skipServerSet the server shouldn't belong to this set
   */
  protected ServerName getOneRandomServer(String rack, Set<ServerName> skipServerSet) {

    // Is the rack valid? Do we recognize it?
    if (rack == null || getServersFromRack(rack) == null ||
        getServersFromRack(rack).isEmpty()) {
      return null;
    }

    // Lets use a set so we can eliminate duplicates
    Set<StartcodeAgnosticServerName> serversToChooseFrom = Sets.newHashSet();
    for (ServerName sn : getServersFromRack(rack)) {
      serversToChooseFrom.add(StartcodeAgnosticServerName.valueOf(sn));
    }

    if (skipServerSet != null && skipServerSet.size() > 0) {
      for (ServerName sn : skipServerSet) {
        serversToChooseFrom.remove(StartcodeAgnosticServerName.valueOf(sn));
      }
      // Do we have any servers left to choose from?
      if (serversToChooseFrom.isEmpty()) {
        return null;
      }
    }

    ServerName randomServer = null;
    int randomIndex = random.nextInt(serversToChooseFrom.size());
    int j = 0;
    for (StartcodeAgnosticServerName sn : serversToChooseFrom) {
      if (j == randomIndex) {
        randomServer = sn;
        break;
      }
      j++;
    }

    if (randomServer != null) {
      return ServerName.valueOf(randomServer.getAddress(), randomServer.getStartcode());
    } else {
      return null;
    }
  }

  private ServerName getOneRandomServer(String rack) throws IOException {
    return this.getOneRandomServer(rack, null);
  }

  protected String getOneRandomRack(Set<String> skipRackSet) throws IOException {
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

  public static String getFavoredNodesAsString(List<ServerName> nodes) {
    StringBuilder strBuf = new StringBuilder();
    int i = 0;
    for (ServerName node : nodes) {
      strBuf.append(node.getAddress());
      if (++i != nodes.size()) strBuf.append(";");
    }
    return strBuf.toString();
  }

  /*
   * Generates a missing favored node based on the input favored nodes. This helps to generate
   * new FN when there is already 2 FN and we need a third one. For eg, while generating new FN
   * for split daughters after inheriting 2 FN from the parent. If the cluster has only one rack
   * it generates from the same rack. If the cluster has multiple racks, then it ensures the new
   * FN respects the rack constraints similar to HDFS. For eg: if there are 3 FN, they will be
   * spread across 2 racks.
   */
  public ServerName generateMissingFavoredNode(List<ServerName> favoredNodes) throws IOException {
    if (this.uniqueRackList.size() == 1) {
      return generateMissingFavoredNodeSingleRack(favoredNodes, null);
    } else {
      return generateMissingFavoredNodeMultiRack(favoredNodes, null);
    }
  }

  public ServerName generateMissingFavoredNode(List<ServerName> favoredNodes,
      List<ServerName> excludeNodes) throws IOException {
    if (this.uniqueRackList.size() == 1) {
      return generateMissingFavoredNodeSingleRack(favoredNodes, excludeNodes);
    } else {
      return generateMissingFavoredNodeMultiRack(favoredNodes, excludeNodes);
    }
  }

  /*
   * Generate FN for a single rack scenario, don't generate from one of the excluded nodes. Helps
   * when we would like to find a replacement node.
   */
  private ServerName generateMissingFavoredNodeSingleRack(List<ServerName> favoredNodes,
      List<ServerName> excludeNodes) throws IOException {
    ServerName newServer = null;
    Set<ServerName> excludeFNSet = Sets.newHashSet(favoredNodes);
    if (excludeNodes != null && excludeNodes.size() > 0) {
      excludeFNSet.addAll(excludeNodes);
    }
    if (favoredNodes.size() < FAVORED_NODES_NUM) {
      newServer = this.getOneRandomServer(this.uniqueRackList.get(0), excludeFNSet);
    }
    return newServer;
  }

  private ServerName generateMissingFavoredNodeMultiRack(List<ServerName> favoredNodes)
      throws IOException {
    return generateMissingFavoredNodeMultiRack(favoredNodes, null);
  }

  /*
   * Generates a missing FN based on the input favoredNodes and also the nodes to be skipped.
   *
   * Get the current layout of favored nodes arrangement and nodes to be excluded and get a
   * random node that goes with HDFS block placement. Eg: If the existing nodes are on one rack,
   * generate one from another rack. We exclude as much as possible so the random selection
   * has more chance to generate a node within a few iterations, ideally 1.
   */
  private ServerName generateMissingFavoredNodeMultiRack(List<ServerName> favoredNodes,
      List<ServerName> excludeNodes) throws IOException {

    Set<String> racks = Sets.newHashSet();
    Map<String, Set<ServerName>> rackToFNMapping = new HashMap<>();

    // Lets understand the current rack distribution of the FN
    for (ServerName sn : favoredNodes) {
      String rack = getRackOfServer(sn);
      racks.add(rack);

      Set<ServerName> serversInRack = rackToFNMapping.get(rack);
      if (serversInRack == null) {
        serversInRack = Sets.newHashSet();
        rackToFNMapping.put(rack, serversInRack);
      }
      serversInRack.add(sn);
    }

    // What racks should be skipped while getting a FN?
    Set<String> skipRackSet = Sets.newHashSet();

    /*
     * If both the FN are from the same rack, then we don't want to generate another FN on the
     * same rack. If that rack fails, the region would be unavailable.
     */
    if (racks.size() == 1 && favoredNodes.size() > 1) {
      skipRackSet.add(racks.iterator().next());
    }

    /*
     * If there are no free nodes on the existing racks, we should skip those racks too. We can
     * reduce the number of iterations for FN selection.
     */
    for (String rack : racks) {
      if (getServersFromRack(rack) != null &&
        rackToFNMapping.get(rack).size() == getServersFromRack(rack).size()) {
        skipRackSet.add(rack);
      }
    }

    Set<ServerName> favoredNodeSet = Sets.newHashSet(favoredNodes);
    if (excludeNodes != null && excludeNodes.size() > 0) {
      favoredNodeSet.addAll(excludeNodes);
    }

    /*
     * Lets get a random rack by excluding skipRackSet and generate a random FN from that rack.
     */
    int i = 0;
    Set<String> randomRacks = Sets.newHashSet();
    ServerName newServer = null;
    do {
      String randomRack = this.getOneRandomRack(skipRackSet);
      newServer = this.getOneRandomServer(randomRack, favoredNodeSet);
      randomRacks.add(randomRack);
      i++;
    } while ((i < MAX_ATTEMPTS_FN_GENERATION) && (newServer == null));

    if (newServer == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Unable to generate additional favored nodes for %s after "
            + "considering racks %s and skip rack %s with a unique rack list of %s and rack "
            + "to RS map of %s and RS to rack map of %s",
          StringUtils.join(favoredNodes, ","), randomRacks, skipRackSet, uniqueRackList,
          rackToRegionServerMap, regionServerToRackMap));
      }
      throw new IOException(" Unable to generate additional favored nodes for "
          + StringUtils.join(favoredNodes, ","));
    }
    return newServer;
  }

  /*
   * Generate favored nodes for a region.
   *
   * Choose a random server as primary and then choose secondary and tertiary FN so its spread
   * across two racks.
   */
  public List<ServerName> generateFavoredNodes(RegionInfo hri) throws IOException {

    List<ServerName> favoredNodesForRegion = new ArrayList<>(FAVORED_NODES_NUM);
    ServerName primary = servers.get(random.nextInt(servers.size()));
    favoredNodesForRegion.add(ServerName.valueOf(primary.getAddress(), ServerName.NON_STARTCODE));

    Map<RegionInfo, ServerName> primaryRSMap = new HashMap<>(1);
    primaryRSMap.put(hri, primary);
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryRSMap =
        placeSecondaryAndTertiaryRS(primaryRSMap);
    ServerName[] secondaryAndTertiaryNodes = secondaryAndTertiaryRSMap.get(hri);
    if (secondaryAndTertiaryNodes != null && secondaryAndTertiaryNodes.length == 2) {
      for (ServerName sn : secondaryAndTertiaryNodes) {
        favoredNodesForRegion.add(ServerName.valueOf(sn.getAddress(), ServerName.NON_STARTCODE));
      }
      return favoredNodesForRegion;
    } else {
      throw new HBaseIOException("Unable to generate secondary and tertiary favored nodes.");
    }
  }

  public Map<RegionInfo, List<ServerName>> generateFavoredNodesRoundRobin(
      Map<ServerName, List<RegionInfo>> assignmentMap, List<RegionInfo> regions)
      throws IOException {

    if (regions.size() > 0) {
      if (canPlaceFavoredNodes()) {
        Map<RegionInfo, ServerName> primaryRSMap = new HashMap<>();
        // Lets try to have an equal distribution for primary favored node
        placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);
        return generateFavoredNodes(primaryRSMap);

      } else {
        throw new HBaseIOException("Not enough nodes to generate favored nodes");
      }
    }
    return null;
  }

  /*
   * Generate favored nodes for a set of regions when we know where they are currently hosted.
   */
  private Map<RegionInfo, List<ServerName>> generateFavoredNodes(
      Map<RegionInfo, ServerName> primaryRSMap) {

    Map<RegionInfo, List<ServerName>> generatedFavNodes = new HashMap<>();
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryRSMap =
      placeSecondaryAndTertiaryRS(primaryRSMap);

    for (Entry<RegionInfo, ServerName> entry : primaryRSMap.entrySet()) {
      List<ServerName> favoredNodesForRegion = new ArrayList<>(FAVORED_NODES_NUM);
      RegionInfo region = entry.getKey();
      ServerName primarySN = entry.getValue();
      favoredNodesForRegion.add(ServerName.valueOf(primarySN.getHostname(), primarySN.getPort(),
        NON_STARTCODE));
      ServerName[] secondaryAndTertiaryNodes = secondaryAndTertiaryRSMap.get(region);
      if (secondaryAndTertiaryNodes != null) {
        favoredNodesForRegion.add(ServerName.valueOf(
          secondaryAndTertiaryNodes[0].getHostname(), secondaryAndTertiaryNodes[0].getPort(),
          NON_STARTCODE));
        favoredNodesForRegion.add(ServerName.valueOf(
          secondaryAndTertiaryNodes[1].getHostname(), secondaryAndTertiaryNodes[1].getPort(),
          NON_STARTCODE));
      }
      generatedFavNodes.put(region, favoredNodesForRegion);
    }
    return generatedFavNodes;
  }

  /*
   * Get the rack of server from local mapping when present, saves lookup by the RackManager.
   */
  private String getRackOfServer(ServerName sn) {
    if (this.regionServerToRackMap.containsKey(sn.getHostname())) {
      return this.regionServerToRackMap.get(sn.getHostname());
    } else {
      String rack = this.rackManager.getRack(sn);
      this.regionServerToRackMap.put(sn.getHostname(), rack);
      return rack;
    }
  }
}

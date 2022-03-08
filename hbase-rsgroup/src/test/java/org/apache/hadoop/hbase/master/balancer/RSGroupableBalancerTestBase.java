/**
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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Base UT of RSGroupableBalancer.
 */
public class RSGroupableBalancerTestBase extends BalancerTestBase{

  static String[] groups = new String[] {RSGroupInfo.DEFAULT_GROUP, "dg2", "dg3", "dg4"};
  static TableName table0 = TableName.valueOf("dt0");
  static TableName[] tables =
      new TableName[] { TableName.valueOf("dt1"),
          TableName.valueOf("dt2"),
          TableName.valueOf("dt3"),
          TableName.valueOf("dt4")};
  static List<ServerName> servers;
  static Map<String, RSGroupInfo> groupMap;
  static Map<TableName, String> tableMap = new HashMap<>();
  static List<TableDescriptor> tableDescs;
  int[] regionAssignment = new int[] { 2, 5, 7, 10, 4, 3, 1 };
  static int regionId = 0;

  /**
   * Invariant is that all servers of a group have load between floor(avg) and
   * ceiling(avg) number of regions.
   */
  protected void assertClusterAsBalanced(
      ArrayListMultimap<String, ServerAndLoad> groupLoadMap) {
    for (String gName : groupLoadMap.keySet()) {
      List<ServerAndLoad> groupLoad = groupLoadMap.get(gName);
      int numServers = groupLoad.size();
      int numRegions = 0;
      int maxRegions = 0;
      int minRegions = Integer.MAX_VALUE;
      for (ServerAndLoad server : groupLoad) {
        int nr = server.getLoad();
        if (nr > maxRegions) {
          maxRegions = nr;
        }
        if (nr < minRegions) {
          minRegions = nr;
        }
        numRegions += nr;
      }
      if (maxRegions - minRegions < 2) {
        // less than 2 between max and min, can't balance
        return;
      }
      int min = numRegions / numServers;
      int max = numRegions % numServers == 0 ? min : min + 1;

      for (ServerAndLoad server : groupLoad) {
        assertTrue(server.getLoad() <= max);
        assertTrue(server.getLoad() >= min);
      }
    }
  }

  /**
   * All regions have an assignment.
   */
  protected void assertImmediateAssignment(List<RegionInfo> regions,
                                         List<ServerName> servers,
                                         Map<RegionInfo, ServerName> assignments)
      throws IOException {
    for (RegionInfo region : regions) {
      assertTrue(assignments.containsKey(region));
      ServerName server = assignments.get(region);
      TableName tableName = region.getTable();

      String groupName = getMockedGroupInfoManager().getRSGroupOfTable(tableName);
      assertTrue(StringUtils.isNotEmpty(groupName));
      RSGroupInfo gInfo = getMockedGroupInfoManager().getRSGroup(groupName);
      assertTrue("Region is not correctly assigned to group servers.",
          gInfo.containsServer(server.getAddress()));
    }
  }

  /**
   * Asserts a valid retained assignment plan.
   * <p>
   * Must meet the following conditions:
   * <ul>
   * <li>Every input region has an assignment, and to an online server
   * <li>If a region had an existing assignment to a server with the same
   * address a a currently online server, it will be assigned to it
   * </ul>
   */
  protected void assertRetainedAssignment(
      Map<RegionInfo, ServerName> existing, List<ServerName> servers,
      Map<ServerName, List<RegionInfo>> assignment)
      throws FileNotFoundException, IOException {
    // Verify condition 1, every region assigned, and to online server
    Set<ServerName> onlineServerSet = new TreeSet<>(servers);
    Set<RegionInfo> assignedRegions = new TreeSet<>(RegionInfo.COMPARATOR);
    for (Map.Entry<ServerName, List<RegionInfo>> a : assignment.entrySet()) {
      assertTrue(
          "Region assigned to server that was not listed as online",
          onlineServerSet.contains(a.getKey()));
      for (RegionInfo r : a.getValue()) {
        assignedRegions.add(r);
      }
    }
    assertEquals(existing.size(), assignedRegions.size());

    // Verify condition 2, every region must be assigned to correct server.
    Set<String> onlineHostNames = new TreeSet<>();
    for (ServerName s : servers) {
      onlineHostNames.add(s.getHostname());
    }

    for (Map.Entry<ServerName, List<RegionInfo>> a : assignment.entrySet()) {
      ServerName currentServer = a.getKey();
      for (RegionInfo r : a.getValue()) {
        ServerName oldAssignedServer = existing.get(r);
        TableName tableName = r.getTable();
        String groupName =
            getMockedGroupInfoManager().getRSGroupOfTable(tableName);
        assertTrue(StringUtils.isNotEmpty(groupName));
        RSGroupInfo gInfo = getMockedGroupInfoManager().getRSGroup(
            groupName);
        assertTrue(
            "Region is not correctly assigned to group servers.",
            gInfo.containsServer(currentServer.getAddress()));
        if (oldAssignedServer != null
            && onlineHostNames.contains(oldAssignedServer
            .getHostname())) {
          // this region was previously assigned somewhere, and that
          // host is still around, then the host must have been is a
          // different group.
          if (!oldAssignedServer.getAddress().equals(currentServer.getAddress())) {
            assertFalse(gInfo.containsServer(oldAssignedServer.getAddress()));
          }
        }
      }
    }
  }

  protected String printStats(
      ArrayListMultimap<String, ServerAndLoad> groupBasedLoad) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    for (String groupName : groupBasedLoad.keySet()) {
      sb.append("Stats for group: " + groupName);
      sb.append("\n");
      sb.append(groupMap.get(groupName).getServers());
      sb.append("\n");
      List<ServerAndLoad> groupLoad = groupBasedLoad.get(groupName);
      int numServers = groupLoad.size();
      int totalRegions = 0;
      sb.append("Per Server Load: \n");
      for (ServerAndLoad sLoad : groupLoad) {
        sb.append("Server :" + sLoad.getServerName() + " Load : "
            + sLoad.getLoad() + "\n");
        totalRegions += sLoad.getLoad();
      }
      sb.append(" Group Statistics : \n");
      float average = (float) totalRegions / numServers;
      int max = (int) Math.ceil(average);
      int min = (int) Math.floor(average);
      sb.append("[srvr=" + numServers + " rgns=" + totalRegions + " avg="
          + average + " max=" + max + " min=" + min + "]");
      sb.append("\n");
      sb.append("===============================");
      sb.append("\n");
    }
    return sb.toString();
  }

  protected ArrayListMultimap<String, ServerAndLoad> convertToGroupBasedMap(
      final Map<ServerName, List<RegionInfo>> serversMap) throws IOException {
    ArrayListMultimap<String, ServerAndLoad> loadMap = ArrayListMultimap
        .create();
    for (RSGroupInfo gInfo : getMockedGroupInfoManager().listRSGroups()) {
      Set<Address> groupServers = gInfo.getServers();
      for (Address hostPort : groupServers) {
        ServerName actual = null;
        for(ServerName entry: servers) {
          if(entry.getAddress().equals(hostPort)) {
            actual = entry;
            break;
          }
        }
        List<RegionInfo> regions = serversMap.get(actual);
        assertTrue("No load for " + actual, regions != null);
        loadMap.put(gInfo.getName(),
            new ServerAndLoad(actual, regions.size()));
      }
    }
    return loadMap;
  }

  protected ArrayListMultimap<String, ServerAndLoad> reconcile(
      ArrayListMultimap<String, ServerAndLoad> previousLoad,
      List<RegionPlan> plans) {
    ArrayListMultimap<String, ServerAndLoad> result = ArrayListMultimap
        .create();
    result.putAll(previousLoad);
    if (plans != null) {
      for (RegionPlan plan : plans) {
        ServerName source = plan.getSource();
        updateLoad(result, source, -1);
        ServerName destination = plan.getDestination();
        updateLoad(result, destination, +1);
      }
    }
    return result;
  }

  protected void updateLoad(
      ArrayListMultimap<String, ServerAndLoad> previousLoad,
      final ServerName sn, final int diff) {
    for (String groupName : previousLoad.keySet()) {
      ServerAndLoad newSAL = null;
      ServerAndLoad oldSAL = null;
      for (ServerAndLoad sal : previousLoad.get(groupName)) {
        if (ServerName.isSameAddress(sn, sal.getServerName())) {
          oldSAL = sal;
          newSAL = new ServerAndLoad(sn, sal.getLoad() + diff);
          break;
        }
      }
      if (newSAL != null) {
        previousLoad.remove(groupName, oldSAL);
        previousLoad.put(groupName, newSAL);
        break;
      }
    }
  }

  protected Map<ServerName, List<RegionInfo>> mockClusterServers() throws IOException {
    assertTrue(servers.size() == regionAssignment.length);
    Map<ServerName, List<RegionInfo>> assignment = new TreeMap<>();
    for (int i = 0; i < servers.size(); i++) {
      int numRegions = regionAssignment[i];
      List<RegionInfo> regions = assignedRegions(numRegions, servers.get(i));
      assignment.put(servers.get(i), regions);
    }
    return assignment;
  }

  /**
   * Generate a list of regions evenly distributed between the tables.
   *
   * @param numRegions The number of regions to be generated.
   * @return List of RegionInfo.
   */
  protected List<RegionInfo> randomRegions(int numRegions) {
    List<RegionInfo> regions = new ArrayList<>(numRegions);
    byte[] start = new byte[16];
    Bytes.random(start);
    byte[] end = new byte[16];
    Bytes.random(end);
    int regionIdx = ThreadLocalRandom.current().nextInt(tables.length);
    for (int i = 0; i < numRegions; i++) {
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      int tableIndex = (i + regionIdx) % tables.length;
      regions.add(RegionInfoBuilder.newBuilder(tables[tableIndex])
          .setStartKey(start)
          .setEndKey(end)
          .setSplit(false)
          .setRegionId(regionId++)
          .build());
    }
    return regions;
  }

  /**
   * Generate assigned regions to a given server using group information.
   *
   * @param numRegions the num regions to generate
   * @param sn the servername
   * @return the list of regions
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  protected List<RegionInfo> assignedRegions(int numRegions, ServerName sn) throws IOException {
    List<RegionInfo> regions = new ArrayList<>(numRegions);
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    Bytes.putInt(start, 0, numRegions << 1);
    Bytes.putInt(end, 0, (numRegions << 1) + 1);
    for (int i = 0; i < numRegions; i++) {
      TableName tableName = getTableName(sn);
      regions.add(RegionInfoBuilder.newBuilder(tableName)
          .setStartKey(start)
          .setEndKey(end)
          .setSplit(false)
          .setRegionId(regionId++)
          .build());
    }
    return regions;
  }

  protected static List<ServerName> generateServers(int numServers) {
    List<ServerName> servers = new ArrayList<>(numServers);
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < numServers; i++) {
      String host = "server" + rand.nextInt(100000);
      int port = rand.nextInt(60000);
      servers.add(ServerName.valueOf(host, port, -1));
    }
    return servers;
  }

  /**
   * Construct group info, with each group having at least one server.
   *
   * @param servers the servers
   * @param groups the groups
   * @return the map
   */
  protected static Map<String, RSGroupInfo> constructGroupInfo(
      List<ServerName> servers, String[] groups) {
    assertTrue(servers != null);
    assertTrue(servers.size() >= groups.length);
    int index = 0;
    Map<String, RSGroupInfo> groupMap = new HashMap<>();
    for (String grpName : groups) {
      RSGroupInfo RSGroupInfo = new RSGroupInfo(grpName);
      RSGroupInfo.addServer(servers.get(index).getAddress());
      groupMap.put(grpName, RSGroupInfo);
      index++;
    }
    Random rand = ThreadLocalRandom.current();
    while (index < servers.size()) {
      int grpIndex = rand.nextInt(groups.length);
      groupMap.get(groups[grpIndex]).addServer(
          servers.get(index).getAddress());
      index++;
    }
    return groupMap;
  }

  /**
   * Construct table descriptors evenly distributed between the groups.
   * @param hasBogusTable there is a table that does not determine the group
   * @return the list of table descriptors
   */
  protected static List<TableDescriptor> constructTableDesc(boolean hasBogusTable) {
    List<TableDescriptor> tds = Lists.newArrayList();
    Random rand = ThreadLocalRandom.current();
    int index = rand.nextInt(groups.length);
    for (int i = 0; i < tables.length; i++) {
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tables[i]).build();
      int grpIndex = (i + index) % groups.length;
      String groupName = groups[grpIndex];
      tableMap.put(tables[i], groupName);
      tds.add(htd);
    }
    if (hasBogusTable) {
      tableMap.put(table0, "");
      tds.add(TableDescriptorBuilder.newBuilder(table0).build());
    }
    return tds;
  }

  protected static MasterServices getMockedMaster() throws IOException {
    TableDescriptors tds = Mockito.mock(TableDescriptors.class);
    Mockito.when(tds.get(tables[0])).thenReturn(tableDescs.get(0));
    Mockito.when(tds.get(tables[1])).thenReturn(tableDescs.get(1));
    Mockito.when(tds.get(tables[2])).thenReturn(tableDescs.get(2));
    Mockito.when(tds.get(tables[3])).thenReturn(tableDescs.get(3));
    MasterServices services = Mockito.mock(HMaster.class);
    Mockito.when(services.getTableDescriptors()).thenReturn(tds);
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    Mockito.when(services.getAssignmentManager()).thenReturn(am);
    return services;
  }

  protected static RSGroupInfoManager getMockedGroupInfoManager() throws IOException {
    RSGroupInfoManager gm = Mockito.mock(RSGroupInfoManager.class);
    Mockito.when(gm.getRSGroup(Mockito.any())).thenAnswer(new Answer<RSGroupInfo>() {
      @Override
      public RSGroupInfo answer(InvocationOnMock invocation) throws Throwable {
        return groupMap.get(invocation.getArgument(0));
      }
    });
    Mockito.when(gm.listRSGroups()).thenReturn(
        Lists.newLinkedList(groupMap.values()));
    Mockito.when(gm.isOnline()).thenReturn(true);
    Mockito.when(gm.getRSGroupOfTable(Mockito.any()))
        .thenAnswer(new Answer<String>() {
          @Override
          public String answer(InvocationOnMock invocation) throws Throwable {
            return tableMap.get(invocation.getArgument(0));
          }
        });
    return gm;
  }

  protected TableName getTableName(ServerName sn) throws IOException {
    TableName tableName = null;
    RSGroupInfoManager gm = getMockedGroupInfoManager();
    RSGroupInfo groupOfServer = null;
    for(RSGroupInfo gInfo : gm.listRSGroups()){
      if(gInfo.containsServer(sn.getAddress())){
        groupOfServer = gInfo;
        break;
      }
    }

    for(TableDescriptor desc : tableDescs){
      if(gm.getRSGroupOfTable(desc.getTableName()).endsWith(groupOfServer.getName())){
        tableName = desc.getTableName();
      }
    }
    return tableName;
  }
}

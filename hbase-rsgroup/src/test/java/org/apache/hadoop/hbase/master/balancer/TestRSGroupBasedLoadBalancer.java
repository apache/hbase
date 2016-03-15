/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master.balancer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

//TODO use stochastic based load balancer instead
@Category(SmallTests.class)
public class TestRSGroupBasedLoadBalancer {

  private static final Log LOG = LogFactory.getLog(TestRSGroupBasedLoadBalancer.class);
  private static RSGroupBasedLoadBalancer loadBalancer;
  private static SecureRandom rand;

  static String[]  groups = new String[] { RSGroupInfo.DEFAULT_GROUP, "dg2", "dg3",
      "dg4" };
  static TableName[] tables =
      new TableName[] { TableName.valueOf("dt1"),
          TableName.valueOf("dt2"),
          TableName.valueOf("dt3"),
          TableName.valueOf("dt4")};
  static List<ServerName> servers;
  static Map<String, RSGroupInfo> groupMap;
  static Map<TableName, String> tableMap;
  static List<HTableDescriptor> tableDescs;
  int[] regionAssignment = new int[] { 2, 5, 7, 10, 4, 3, 1 };
  static int regionId = 0;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    rand = new SecureRandom();
    servers = generateServers(7);
    groupMap = constructGroupInfo(servers, groups);
    tableMap = new HashMap<TableName, String>();
    tableDescs = constructTableDesc();
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.regions.slop", "0");
    conf.set("hbase.group.grouploadbalancer.class", SimpleLoadBalancer.class.getCanonicalName());
    loadBalancer = new RSGroupBasedLoadBalancer(getMockedGroupInfoManager());
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.setConf(conf);
    loadBalancer.initialize();
  }

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers of the group should be hosting either floor(average) or
   * ceiling(average)
   *
   * @throws Exception
   */
  @Test
  public void testBalanceCluster() throws Exception {
    Map<ServerName, List<HRegionInfo>> servers = mockClusterServers();
    ArrayListMultimap<String, ServerAndLoad> list = convertToGroupBasedMap(servers);
    LOG.info("Mock Cluster :  " + printStats(list));
    List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
    ArrayListMultimap<String, ServerAndLoad> balancedCluster = reconcile(
        list, plans);
    LOG.info("Mock Balance : " + printStats(balancedCluster));
    assertClusterAsBalanced(balancedCluster);
  }

  /**
   * Invariant is that all servers of a group have load between floor(avg) and
   * ceiling(avg) number of regions.
   */
  private void assertClusterAsBalanced(
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
   *
   * @param regions
   * @param servers
   * @param assignments
   * @throws java.io.IOException
   * @throws java.io.FileNotFoundException
   */
  private void assertImmediateAssignment(List<HRegionInfo> regions,
                                         List<ServerName> servers,
                                         Map<HRegionInfo, ServerName> assignments)
      throws IOException {
    for (HRegionInfo region : regions) {
      assertTrue(assignments.containsKey(region));
      ServerName server = assignments.get(region);
      TableName tableName = region.getTable();

      String groupName =
          getMockedGroupInfoManager().getRSGroupOfTable(tableName);
      assertTrue(StringUtils.isNotEmpty(groupName));
      RSGroupInfo gInfo = getMockedGroupInfoManager().getRSGroup(groupName);
      assertTrue("Region is not correctly assigned to group servers.",
          gInfo.containsServer(server.getHostPort()));
    }
  }

  /**
   * Tests the bulk assignment used during cluster startup.
   *
   * Round-robin. Should yield a balanced cluster so same invariant as the
   * load balancer holds, all servers holding either floor(avg) or
   * ceiling(avg).
   *
   * @throws Exception
   */
  @Test
  public void testBulkAssignment() throws Exception {
    List<HRegionInfo> regions = randomRegions(25);
    Map<ServerName, List<HRegionInfo>> assignments = loadBalancer
        .roundRobinAssignment(regions, servers);
    //test empty region/servers scenario
    //this should not throw an NPE
    loadBalancer.roundRobinAssignment(regions,
        Collections.EMPTY_LIST);
    //test regular scenario
    assertTrue(assignments.keySet().size() == servers.size());
    for (ServerName sn : assignments.keySet()) {
      List<HRegionInfo> regionAssigned = assignments.get(sn);
      for (HRegionInfo region : regionAssigned) {
        TableName tableName = region.getTable();
        String groupName =
            getMockedGroupInfoManager().getRSGroupOfTable(tableName);
        assertTrue(StringUtils.isNotEmpty(groupName));
        RSGroupInfo gInfo = getMockedGroupInfoManager().getRSGroup(
            groupName);
        assertTrue(
            "Region is not correctly assigned to group servers.",
            gInfo.containsServer(sn.getHostPort()));
      }
    }
    ArrayListMultimap<String, ServerAndLoad> loadMap = convertToGroupBasedMap(assignments);
    assertClusterAsBalanced(loadMap);
  }

  /**
   * Test the cluster startup bulk assignment which attempts to retain
   * assignment info.
   *
   * @throws Exception
   */
  @Test
  public void testRetainAssignment() throws Exception {
    // Test simple case where all same servers are there
    Map<ServerName, List<HRegionInfo>> currentAssignments = mockClusterServers();
    Map<HRegionInfo, ServerName> inputForTest = new HashMap<HRegionInfo, ServerName>();
    for (ServerName sn : currentAssignments.keySet()) {
      for (HRegionInfo region : currentAssignments.get(sn)) {
        inputForTest.put(region, sn);
      }
    }
    //verify region->null server assignment is handled
    inputForTest.put(randomRegions(1).get(0), null);
    Map<ServerName, List<HRegionInfo>> newAssignment = loadBalancer
        .retainAssignment(inputForTest, servers);
    assertRetainedAssignment(inputForTest, servers, newAssignment);
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
   *
   * @param existing
   * @param assignment
   * @throws java.io.IOException
   * @throws java.io.FileNotFoundException
   */
  private void assertRetainedAssignment(
      Map<HRegionInfo, ServerName> existing, List<ServerName> servers,
      Map<ServerName, List<HRegionInfo>> assignment)
      throws FileNotFoundException, IOException {
    // Verify condition 1, every region assigned, and to online server
    Set<ServerName> onlineServerSet = new TreeSet<ServerName>(servers);
    Set<HRegionInfo> assignedRegions = new TreeSet<HRegionInfo>();
    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      assertTrue(
          "Region assigned to server that was not listed as online",
          onlineServerSet.contains(a.getKey()));
      for (HRegionInfo r : a.getValue())
        assignedRegions.add(r);
    }
    assertEquals(existing.size(), assignedRegions.size());

    // Verify condition 2, every region must be assigned to correct server.
    Set<String> onlineHostNames = new TreeSet<String>();
    for (ServerName s : servers) {
      onlineHostNames.add(s.getHostname());
    }

    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      ServerName currentServer = a.getKey();
      for (HRegionInfo r : a.getValue()) {
        ServerName oldAssignedServer = existing.get(r);
        TableName tableName = r.getTable();
        String groupName =
            getMockedGroupInfoManager().getRSGroupOfTable(tableName);
        assertTrue(StringUtils.isNotEmpty(groupName));
        RSGroupInfo gInfo = getMockedGroupInfoManager().getRSGroup(
            groupName);
        assertTrue(
            "Region is not correctly assigned to group servers.",
            gInfo.containsServer(currentServer.getHostPort()));
        if (oldAssignedServer != null
            && onlineHostNames.contains(oldAssignedServer
            .getHostname())) {
          // this region was previously assigned somewhere, and that
          // host is still around, then the host must have been is a
          // different group.
          if (!oldAssignedServer.getHostPort().equals(currentServer.getHostPort())) {
            assertFalse(gInfo.containsServer(oldAssignedServer.getHostPort()));
          }
        }
      }
    }
  }

  private String printStats(
      ArrayListMultimap<String, ServerAndLoad> groupBasedLoad) {
    StringBuffer sb = new StringBuffer();
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

  private ArrayListMultimap<String, ServerAndLoad> convertToGroupBasedMap(
      final Map<ServerName, List<HRegionInfo>> serversMap) throws IOException {
    ArrayListMultimap<String, ServerAndLoad> loadMap = ArrayListMultimap
        .create();
    for (RSGroupInfo gInfo : getMockedGroupInfoManager().listRSGroups()) {
      Set<HostAndPort> groupServers = gInfo.getServers();
      for (HostAndPort hostPort : groupServers) {
        ServerName actual = null;
        for(ServerName entry: servers) {
          if(entry.getHostPort().equals(hostPort)) {
            actual = entry;
            break;
          }
        }
        List<HRegionInfo> regions = serversMap.get(actual);
        assertTrue("No load for " + actual, regions != null);
        loadMap.put(gInfo.getName(),
            new ServerAndLoad(actual, regions.size()));
      }
    }
    return loadMap;
  }

  private ArrayListMultimap<String, ServerAndLoad> reconcile(
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

  private void updateLoad(
      ArrayListMultimap<String, ServerAndLoad> previousLoad,
      final ServerName sn, final int diff) {
    for (String groupName : previousLoad.keySet()) {
      ServerAndLoad newSAL = null;
      ServerAndLoad oldSAL = null;
      for (ServerAndLoad sal : previousLoad.get(groupName)) {
        if (ServerName.isSameHostnameAndPort(sn, sal.getServerName())) {
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

  private Map<ServerName, List<HRegionInfo>> mockClusterServers() throws IOException {
    assertTrue(servers.size() == regionAssignment.length);
    Map<ServerName, List<HRegionInfo>> assignment = new TreeMap<ServerName, List<HRegionInfo>>();
    for (int i = 0; i < servers.size(); i++) {
      int numRegions = regionAssignment[i];
      List<HRegionInfo> regions = assignedRegions(numRegions, servers.get(i));
      assignment.put(servers.get(i), regions);
    }
    return assignment;
  }

  /**
   * Generate a list of regions evenly distributed between the tables.
   *
   * @param numRegions The number of regions to be generated.
   * @return List of HRegionInfo.
   */
  private List<HRegionInfo> randomRegions(int numRegions) {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(numRegions);
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    rand.nextBytes(start);
    rand.nextBytes(end);
    int regionIdx = rand.nextInt(tables.length);
    for (int i = 0; i < numRegions; i++) {
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      int tableIndex = (i + regionIdx) % tables.length;
      HRegionInfo hri = new HRegionInfo(
          tables[tableIndex], start, end, false, regionId++);
      regions.add(hri);
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
  private List<HRegionInfo> assignedRegions(int numRegions, ServerName sn) throws IOException {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(numRegions);
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    Bytes.putInt(start, 0, numRegions << 1);
    Bytes.putInt(end, 0, (numRegions << 1) + 1);
    for (int i = 0; i < numRegions; i++) {
      TableName tableName = getTableName(sn);
      HRegionInfo hri = new HRegionInfo(
          tableName, start, end, false,
          regionId++);
      regions.add(hri);
    }
    return regions;
  }

  private static List<ServerName> generateServers(int numServers) {
    List<ServerName> servers = new ArrayList<ServerName>(numServers);
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
  private static Map<String, RSGroupInfo> constructGroupInfo(
      List<ServerName> servers, String[] groups) {
    assertTrue(servers != null);
    assertTrue(servers.size() >= groups.length);
    int index = 0;
    Map<String, RSGroupInfo> groupMap = new HashMap<String, RSGroupInfo>();
    for (String grpName : groups) {
      RSGroupInfo RSGroupInfo = new RSGroupInfo(grpName);
      RSGroupInfo.addServer(servers.get(index).getHostPort());
      groupMap.put(grpName, RSGroupInfo);
      index++;
    }
    while (index < servers.size()) {
      int grpIndex = rand.nextInt(groups.length);
      groupMap.get(groups[grpIndex]).addServer(
          servers.get(index).getHostPort());
      index++;
    }
    return groupMap;
  }

  /**
   * Construct table descriptors evenly distributed between the groups.
   *
   * @return the list
   */
  private static List<HTableDescriptor> constructTableDesc() {
    List<HTableDescriptor> tds = Lists.newArrayList();
    int index = rand.nextInt(groups.length);
    for (int i = 0; i < tables.length; i++) {
      HTableDescriptor htd = new HTableDescriptor(tables[i]);
      int grpIndex = (i + index) % groups.length ;
      String groupName = groups[grpIndex];
      tableMap.put(tables[i], groupName);
      tds.add(htd);
    }
    return tds;
  }

  private static MasterServices getMockedMaster() throws IOException {
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

  private static RSGroupInfoManager getMockedGroupInfoManager() throws IOException {
    RSGroupInfoManager gm = Mockito.mock(RSGroupInfoManager.class);
    Mockito.when(gm.getRSGroup(groups[0])).thenReturn(
        groupMap.get(groups[0]));
    Mockito.when(gm.getRSGroup(groups[1])).thenReturn(
        groupMap.get(groups[1]));
    Mockito.when(gm.getRSGroup(groups[2])).thenReturn(
        groupMap.get(groups[2]));
    Mockito.when(gm.getRSGroup(groups[3])).thenReturn(
        groupMap.get(groups[3]));
    Mockito.when(gm.listRSGroups()).thenReturn(
        Lists.newLinkedList(groupMap.values()));
    Mockito.when(gm.isOnline()).thenReturn(true);
    Mockito.when(gm.getRSGroupOfTable(Mockito.any(TableName.class)))
        .thenAnswer(new Answer<String>() {
          @Override
          public String answer(InvocationOnMock invocation) throws Throwable {
            return tableMap.get(invocation.getArguments()[0]);
          }
        });
    return gm;
  }

  private TableName getTableName(ServerName sn) throws IOException {
    TableName tableName = null;
    RSGroupInfoManager gm = getMockedGroupInfoManager();
    RSGroupInfo groupOfServer = null;
    for(RSGroupInfo gInfo : gm.listRSGroups()){
      if(gInfo.containsServer(sn.getHostPort())){
        groupOfServer = gInfo;
        break;
      }
    }

    for(HTableDescriptor desc : tableDescs){
      if(gm.getRSGroupOfTable(desc.getTableName()).endsWith(groupOfServer.getName())){
        tableName = desc.getTableName();
      }
    }
    return tableName;
  }
}

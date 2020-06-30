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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.HbckChore;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MasterTests.class, MediumTests.class })
public class TestHbckChore extends TestAssignmentManagerBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHbckChore.class);

  private HbckChore hbckChore;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    hbckChore = new HbckChore(master);
  }

  @Test
  public void testForMeta() {
    RegionInfo meta = am.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).get(0);
    byte[] metaRegionNameAsBytes = meta.getRegionName();
    String metaRegionName = meta.getRegionNameAsString();
    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
        hbckChore.getInconsistentRegions();

    // Test for case1: Master thought this region opened, but no regionserver reported it.
    assertTrue(inconsistentRegions.containsKey(metaRegionName));
    Pair<ServerName, List<ServerName>> pair = inconsistentRegions.get(metaRegionName);
    ServerName locationInMeta = pair.getFirst();
    List<ServerName> reportedRegionServers = pair.getSecond();
    assertTrue(serverNames.contains(locationInMeta));
    assertEquals(0, reportedRegionServers.size());

    // Reported right region location. Then not in problematic regions.
    am.reportOnlineRegions(locationInMeta, Collections.singleton(metaRegionNameAsBytes));
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(metaRegionName));
  }

  @Test
  public void testForUserTable() throws Exception {
    TableName tableName = TableName.valueOf("testForUserTable");
    RegionInfo hri = createRegionInfo(tableName, 1);
    String regionName = hri.getRegionNameAsString();
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    Future<byte[]> future = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(future);

    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    // Test for case1: Master thought this region opened, but no regionserver reported it.
    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
        hbckChore.getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    Pair<ServerName, List<ServerName>> pair = inconsistentRegions.get(regionName);
    ServerName locationInMeta = pair.getFirst();
    List<ServerName> reportedRegionServers = pair.getSecond();
    assertTrue(serverNames.contains(locationInMeta));
    assertEquals(0, reportedRegionServers.size());

    // Test for case2: Master thought this region opened on Server1, but regionserver reported
    // Server2
    final ServerName tempLocationInMeta = locationInMeta;
    final ServerName anotherServer =
        serverNames.stream().filter(s -> !s.equals(tempLocationInMeta)).findFirst().get();
    am.reportOnlineRegions(anotherServer, Collections.singleton(hri.getRegionName()));
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    pair = inconsistentRegions.get(regionName);
    locationInMeta = pair.getFirst();
    reportedRegionServers = pair.getSecond();
    assertEquals(1, reportedRegionServers.size());
    assertFalse(reportedRegionServers.contains(locationInMeta));
    assertTrue(reportedRegionServers.contains(anotherServer));

    // Test for case3: More than one regionservers reported opened this region.
    am.reportOnlineRegions(locationInMeta, Collections.singleton(hri.getRegionName()));
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    pair = inconsistentRegions.get(regionName);
    locationInMeta = pair.getFirst();
    reportedRegionServers = pair.getSecond();
    assertEquals(2, reportedRegionServers.size());
    assertTrue(reportedRegionServers.contains(locationInMeta));
    assertTrue(reportedRegionServers.contains(anotherServer));

    // Reported right region location, then not in inconsistent regions.
    am.reportOnlineRegions(anotherServer, Collections.emptySet());
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(regionName));
  }

  @Test
  public void testForDisabledTable() throws Exception {
    TableName tableName = TableName.valueOf("testForDisabledTable");
    RegionInfo hri = createRegionInfo(tableName, 1);
    String regionName = hri.getRegionNameAsString();
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    Future<byte[]> future = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(future);

    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
        hbckChore.getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    Pair<ServerName, List<ServerName>> pair = inconsistentRegions.get(regionName);
    ServerName locationInMeta = pair.getFirst();
    List<ServerName> reportedRegionServers = pair.getSecond();
    assertTrue(serverNames.contains(locationInMeta));
    assertEquals(0, reportedRegionServers.size());

    // Set table state to disabled, then not in inconsistent regions.
    TableStateManager tableStateManager = master.getTableStateManager();
    Mockito.when(tableStateManager.isTableState(tableName, TableState.State.DISABLED)).
        thenReturn(true);
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(regionName));
  }

  @Test
  public void testForSplitParent() throws Exception {
    TableName tableName = TableName.valueOf("testForSplitParent");
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes(0))
        .setEndKey(Bytes.toBytes(1)).setSplit(true).setOffline(true).setRegionId(0).build();
    String regionName = hri.getEncodedName();
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    Future<byte[]> future = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(future);

    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
        hbckChore.getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(regionName));
  }

  @Test
  public void testOrphanRegionsOnFS() throws Exception {
    TableName tableName = TableName.valueOf("testOrphanRegionsOnFS");
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    Configuration conf = util.getConfiguration();

    hbckChore.choreForTesting();
    assertEquals(0, hbckChore.getOrphanRegionsOnFS().size());

    HRegion.createRegionDir(conf, regionInfo, CommonFSUtils.getRootDir(conf));
    hbckChore.choreForTesting();
    assertEquals(1, hbckChore.getOrphanRegionsOnFS().size());
    assertTrue(hbckChore.getOrphanRegionsOnFS().containsKey(regionInfo.getEncodedName()));

    FSUtils.deleteRegionDir(conf, regionInfo);
    hbckChore.choreForTesting();
    assertEquals(0, hbckChore.getOrphanRegionsOnFS().size());
  }

  @Test
  public void testChoreDisable() {
    // The way to disable to chore is to set hbase.master.hbck.chore.interval <= 0
    // When the interval is > 0, the chore should run.
    long lastRunTime = hbckChore.getCheckingEndTimestamp();
    hbckChore.choreForTesting();
    boolean ran = lastRunTime != hbckChore.getCheckingEndTimestamp();
    assertTrue(ran);

    // When the interval <= 0, the chore shouldn't run
    master.getConfiguration().setInt("hbase.master.hbck.chore.interval", 0);
    HbckChore hbckChoreWithChangedConf = new HbckChore(master);
    lastRunTime = hbckChoreWithChangedConf.getCheckingEndTimestamp();
    hbckChoreWithChangedConf.choreForTesting();
    ran = lastRunTime != hbckChoreWithChangedConf.getCheckingEndTimestamp();
    assertFalse(ran);
  }
}
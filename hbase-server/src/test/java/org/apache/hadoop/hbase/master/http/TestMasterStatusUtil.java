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
package org.apache.hadoop.hbase.master.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MasterTests.class, SmallTests.class })
public class TestMasterStatusUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterStatusUtil.class);

  private HMaster master;
  private Configuration conf;

  static final ServerName FAKE_HOST = ServerName.valueOf("fakehost", 12345, 1234567890);
  static final TableDescriptor FAKE_TABLE =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("mytable")).build();

  static final RegionInfo FAKE_HRI = RegionInfoBuilder.newBuilder(FAKE_TABLE.getTableName())
    .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build();

  @Before
  public void setupBasicMocks() {
    conf = HBaseConfiguration.create();

    master = Mockito.mock(HMaster.class);
    Mockito.doReturn(FAKE_HOST).when(master).getServerName();
    Mockito.doReturn(conf).when(master).getConfiguration();
    Mockito.doReturn(true).when(master).isInitialized();

    // Fake DeadServer
    DeadServer deadServer = Mockito.mock(DeadServer.class);
    // Fake serverManager
    ServerManager serverManager = Mockito.mock(ServerManager.class);
    Mockito.doReturn(1.0).when(serverManager).getAverageLoad();
    Mockito.doReturn(serverManager).when(master).getServerManager();
    Mockito.doReturn(deadServer).when(serverManager).getDeadServers();

    // Fake AssignmentManager and RIT
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    RegionStates rs = Mockito.mock(RegionStates.class);
    List<RegionState> regionsInTransition = new ArrayList<>();
    regionsInTransition
      .add(new RegionState(FAKE_HRI, RegionState.State.CLOSING, 12345L, FAKE_HOST));
    Mockito.doReturn(rs).when(am).getRegionStates();
    Mockito.doReturn(regionsInTransition).when(rs).getRegionsInTransition();
    Mockito.doReturn(am).when(master).getAssignmentManager();
    Mockito.doReturn(serverManager).when(master).getServerManager();

    // Fake ZKW
    ZKWatcher zkw = Mockito.mock(ZKWatcher.class);
    Mockito.doReturn(new ZNodePaths(conf)).when(zkw).getZNodePaths();
    Mockito.doReturn("fakequorum").when(zkw).getQuorum();
    Mockito.doReturn(zkw).when(master).getZooKeeper();

    // Fake ActiveMaster
    Mockito.doReturn(Optional.of(FAKE_HOST)).when(master).getActiveMaster();
  }

  @Test
  public void testGetUserTables() throws IOException {
    Map<String, TableDescriptor> mockTables = new HashMap<>();
    mockTables.put("foo", TableDescriptorBuilder.newBuilder(TableName.valueOf("foo")).build());
    mockTables.put("bar", TableDescriptorBuilder.newBuilder(TableName.valueOf("bar")).build());

    TableDescriptors tableDescriptors = Mockito.mock(TableDescriptors.class);
    Mockito.doReturn(tableDescriptors).when(master).getTableDescriptors();

    Mockito.doReturn(mockTables).when(tableDescriptors).getAll();

    List<TableDescriptor> tables = new ArrayList<>();

    String errorMessage = MasterStatusUtil.getUserTables(master, tables);

    assertNull(errorMessage);
    assertEquals(2, tables.size());
  }

  @Test
  public void testGetUserTablesFilterOutSystemTables() throws IOException {
    Map<String, TableDescriptor> mockTables = new HashMap<>();
    mockTables.put("foo", TableDescriptorBuilder.newBuilder(TableName.valueOf("foo")).build());
    mockTables.put("bar", TableDescriptorBuilder.newBuilder(TableName.valueOf("bar")).build());
    mockTables.put("meta",
      TableDescriptorBuilder.newBuilder(TableName.valueOf("hbase", "meta")).build());

    TableDescriptors tableDescriptors = Mockito.mock(TableDescriptors.class);
    Mockito.doReturn(tableDescriptors).when(master).getTableDescriptors();

    Mockito.doReturn(mockTables).when(tableDescriptors).getAll();

    List<TableDescriptor> tables = new ArrayList<>();

    String errorMessage = MasterStatusUtil.getUserTables(master, tables);

    assertNull(errorMessage);
    assertEquals(2, tables.size());
  }

  @Test
  public void testGetUserTablesNoUserTables() throws IOException {
    Map<Object, Object> emptyMap = Collections.emptyMap();

    TableDescriptors tableDescriptors = Mockito.mock(TableDescriptors.class);
    Mockito.doReturn(tableDescriptors).when(master).getTableDescriptors();

    Mockito.doReturn(emptyMap).when(tableDescriptors).getAll();

    List<TableDescriptor> tables = new ArrayList<>();

    String errorMessage = MasterStatusUtil.getUserTables(master, tables);

    assertNull(errorMessage);
    assertEquals(0, tables.size());
  }

  @Test
  public void testGetUserTablesMasterNotInitialized() {
    Mockito.doReturn(false).when(master).isInitialized();

    List<TableDescriptor> tables = new ArrayList<>();

    String errorMessage = MasterStatusUtil.getUserTables(master, tables);

    assertNull(errorMessage);
    assertEquals(0, tables.size());
  }

  @Test
  public void testGetUserTablesException() throws IOException {
    TableDescriptors tableDescriptors = Mockito.mock(TableDescriptors.class);
    Mockito.doReturn(tableDescriptors).when(master).getTableDescriptors();

    Mockito.doThrow(new IOException("some error")).when(tableDescriptors).getAll();

    List<TableDescriptor> tables = new ArrayList<>();
    String errorMessage = MasterStatusUtil.getUserTables(master, tables);
    assertEquals("Got user tables error, some error", errorMessage);
  }

  @Test
  public void testGetFragmentationInfoTurnedOff() throws IOException {
    conf.setBoolean("hbase.master.ui.fragmentation.enabled", false);
    assertNull(MasterStatusUtil.getFragmentationInfo(master, conf));
  }

  @Test
  public void testGetFragmentationInfoTurnedOn() throws IOException {
    conf.setBoolean("hbase.master.ui.fragmentation.enabled", true);
    Map<String, Integer> fragmentationInfo = MasterStatusUtil.getFragmentationInfo(master, conf);
    assertNotNull(fragmentationInfo);
    assertFalse(fragmentationInfo.isEmpty());
  }

  @Test
  public void testGetMetaLocationOrNull() {
    mockMetaLocation(true);

    ServerName location = MasterStatusUtil.getMetaLocationOrNull(master);
    assertNotNull(location);
    assertEquals(FAKE_HOST, location);
  }

  @Test
  public void testGetMetaLocationOrNullNotOpen() {
    mockMetaLocation(false);

    ServerName location = MasterStatusUtil.getMetaLocationOrNull(master);
    assertNull(location);
  }

  private void mockMetaLocation(boolean isOpen) {
    RegionStates rs = master.getAssignmentManager().getRegionStates();
    RegionStateNode rsn = Mockito.mock(RegionStateNode.class);
    Mockito.doReturn(rsn).when(rs).getRegionStateNode(RegionInfoBuilder.FIRST_META_REGIONINFO);
    Mockito.doReturn(isOpen).when(rsn).isInState(RegionState.State.OPEN);
    if (isOpen) {
      Mockito.doReturn(FAKE_HOST).when(rsn).getRegionLocation();
    }
  }

  @Test
  public void testServerNameLink() {
    Mockito.doReturn(16030).when(master).getRegionServerInfoPort(FAKE_HOST);

    String link = MasterStatusUtil.serverNameLink(master, FAKE_HOST);

    assertNotNull(link);
    assertEquals("<a href=\"//fakehost:16030/rs-status\">fakehost,12345,1234567890</a>", link);
  }

  @Test
  public void testServerNameLinkNoInfoPort() {
    Mockito.doReturn(-1).when(master).getRegionServerInfoPort(FAKE_HOST);

    String link = MasterStatusUtil.serverNameLink(master, FAKE_HOST);

    assertNotNull(link);
    assertEquals("fakehost,12345,1234567890", link);
  }
}

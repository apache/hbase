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
package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test cases for backup system table API
 */
@Category(MediumTests.class)
public class TestBackupSystemTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupSystemTable.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  protected static Configuration conf = UTIL.getConfiguration();
  protected static SingleProcessHBaseCluster cluster;
  protected static Connection conn;
  protected BackupSystemTable table;

  @BeforeClass
  public static void setUp() throws Exception {
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    BackupManager.decorateMasterConfiguration(conf);
    BackupManager.decorateRegionServerConfiguration(conf);
    cluster = UTIL.startMiniCluster();
    conn = UTIL.getConnection();
  }

  @Before
  public void before() throws IOException {
    table = new BackupSystemTable(conn);
  }

  @After
  public void after() {
    if (table != null) {
      table.close();
    }

  }

  @Test
  public void testUpdateReadDeleteBackupStatus() throws IOException {
    BackupInfo ctx = createBackupInfo();
    table.updateBackupInfo(ctx);
    BackupInfo readCtx = table.readBackupInfo(ctx.getBackupId());
    assertTrue(compare(ctx, readCtx));
    // try fake backup id
    readCtx = table.readBackupInfo("fake");
    assertNull(readCtx);
    // delete backup info
    table.deleteBackupInfo(ctx.getBackupId());
    readCtx = table.readBackupInfo(ctx.getBackupId());
    assertNull(readCtx);
    cleanBackupTable();
  }

  @Test
  public void testWriteReadBackupStartCode() throws IOException {
    long code = 100L;
    table.writeBackupStartCode(code, "root");
    String readCode = table.readBackupStartCode("root");
    assertEquals(code, Long.parseLong(readCode));
    cleanBackupTable();
  }

  private void cleanBackupTable() throws IOException {
    Admin admin = UTIL.getAdmin();
    admin.disableTable(BackupSystemTable.getTableName(conf));
    admin.truncateTable(BackupSystemTable.getTableName(conf), true);
    if (admin.isTableDisabled(BackupSystemTable.getTableName(conf))) {
      admin.enableTable(BackupSystemTable.getTableName(conf));
    }
  }

  @Test
  public void testBackupHistory() throws Exception {
    int n = 10;
    List<BackupInfo> list = createBackupInfoList(n);

    // Load data
    for (BackupInfo bc : list) {
      // Make sure we set right status
      bc.setState(BackupState.COMPLETE);
      table.updateBackupInfo(bc);
    }

    // Reverse list for comparison
    Collections.reverse(list);
    List<BackupInfo> history = table.getBackupHistory();
    assertTrue(history.size() == n);

    for (int i = 0; i < n; i++) {
      BackupInfo ctx = list.get(i);
      BackupInfo data = history.get(i);
      assertTrue(compare(ctx, data));
    }

    cleanBackupTable();

  }

  @Test
  public void testBackupDelete() throws Exception {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {
      int n = 10;
      List<BackupInfo> list = createBackupInfoList(n);

      // Load data
      for (BackupInfo bc : list) {
        // Make sure we set right status
        bc.setState(BackupState.COMPLETE);
        table.updateBackupInfo(bc);
      }

      // Verify exists
      for (BackupInfo bc : list) {
        assertNotNull(table.readBackupInfo(bc.getBackupId()));
      }

      // Delete all
      for (BackupInfo bc : list) {
        table.deleteBackupInfo(bc.getBackupId());
      }

      // Verify do not exists
      for (BackupInfo bc : list) {
        assertNull(table.readBackupInfo(bc.getBackupId()));
      }

      cleanBackupTable();
    }

  }

  @Test
  public void testRegionServerLastLogRollResults() throws IOException {
    String[] servers = new String[] { "server1", "server2", "server3" };
    Long[] timestamps = new Long[] { 100L, 102L, 107L };

    // validate the prefix scan in readRegionServerlastLogRollResult will get the right timestamps
    // when a backup root with the same prefix is present
    for (int i = 0; i < servers.length; i++) {
      table.writeRegionServerLastLogRollResult(servers[i], timestamps[i], "root");
      table.writeRegionServerLastLogRollResult(servers[i], timestamps[i], "root/backup");
    }

    HashMap<String, Long> result = table.readRegionServerLastLogRollResult("root");
    assertTrue(servers.length == result.size());
    Set<String> keys = result.keySet();
    String[] keysAsArray = new String[keys.size()];
    keys.toArray(keysAsArray);
    Arrays.sort(keysAsArray);

    for (int i = 0; i < keysAsArray.length; i++) {
      assertEquals(keysAsArray[i], servers[i]);
      Long ts1 = timestamps[i];
      Long ts2 = result.get(keysAsArray[i]);
      assertEquals(ts1, ts2);
    }

    cleanBackupTable();
  }

  @Test
  public void testIncrementalBackupTableSet() throws IOException {
    TreeSet<TableName> tables1 = new TreeSet<>();

    tables1.add(TableName.valueOf("t1"));
    tables1.add(TableName.valueOf("t2"));
    tables1.add(TableName.valueOf("t3"));

    TreeSet<TableName> tables2 = new TreeSet<>();

    tables2.add(TableName.valueOf("t3"));
    tables2.add(TableName.valueOf("t4"));
    tables2.add(TableName.valueOf("t5"));

    table.addIncrementalBackupTableSet(tables1, "root");

    try (BackupSystemTable systemTable = new BackupSystemTable(conn)) {
      TreeSet<TableName> res1 =
        (TreeSet<TableName>) systemTable.getIncrementalBackupTableSet("root");
      assertTrue(tables1.size() == res1.size());
      Iterator<TableName> desc1 = tables1.descendingIterator();
      Iterator<TableName> desc2 = res1.descendingIterator();
      while (desc1.hasNext()) {
        assertEquals(desc1.next(), desc2.next());
      }
      systemTable.addIncrementalBackupTableSet(tables2, "root");
      TreeSet<TableName> res2 =
        (TreeSet<TableName>) systemTable.getIncrementalBackupTableSet("root");
      assertTrue((tables2.size() + tables1.size() - 1) == res2.size());
      tables1.addAll(tables2);
      desc1 = tables1.descendingIterator();
      desc2 = res2.descendingIterator();
      while (desc1.hasNext()) {
        assertEquals(desc1.next(), desc2.next());
      }
    }

    cleanBackupTable();
  }

  @Test
  public void testRegionServerLogTimestampMap() throws IOException {
    TreeSet<TableName> tables = new TreeSet<>();

    tables.add(TableName.valueOf("t1"));
    tables.add(TableName.valueOf("t2"));
    tables.add(TableName.valueOf("t3"));

    HashMap<String, Long> rsTimestampMap = new HashMap<>();

    rsTimestampMap.put("rs1:100", 100L);
    rsTimestampMap.put("rs2:100", 101L);
    rsTimestampMap.put("rs3:100", 103L);

    // validate the prefix scan in readLogTimestampMap will get the right timestamps
    // when a backup root with the same prefix is present
    table.writeRegionServerLogTimestamp(tables, rsTimestampMap, "root");
    table.writeRegionServerLogTimestamp(tables, rsTimestampMap, "root/backup");

    Map<TableName, Map<String, Long>> result = table.readLogTimestampMap("root");

    assertTrue(tables.size() == result.size());

    for (TableName t : tables) {
      Map<String, Long> rstm = result.get(t);
      assertNotNull(rstm);
      assertEquals(rstm.get("rs1:100"), Long.valueOf(100L));
      assertEquals(rstm.get("rs2:100"), Long.valueOf(101L));
      assertEquals(rstm.get("rs3:100"), Long.valueOf(103L));
    }

    Set<TableName> tables1 = new TreeSet<>();

    tables1.add(TableName.valueOf("t3"));
    tables1.add(TableName.valueOf("t4"));
    tables1.add(TableName.valueOf("t5"));

    HashMap<String, Long> rsTimestampMap1 = new HashMap<>();

    rsTimestampMap1.put("rs1:100", 200L);
    rsTimestampMap1.put("rs2:100", 201L);
    rsTimestampMap1.put("rs3:100", 203L);

    // validate the prefix scan in readLogTimestampMap will get the right timestamps
    // when a backup root with the same prefix is present
    table.writeRegionServerLogTimestamp(tables1, rsTimestampMap1, "root");
    table.writeRegionServerLogTimestamp(tables1, rsTimestampMap, "root/backup");

    result = table.readLogTimestampMap("root");

    assertTrue(5 == result.size());

    for (TableName t : tables) {
      Map<String, Long> rstm = result.get(t);
      assertNotNull(rstm);
      if (t.equals(TableName.valueOf("t3")) == false) {
        assertEquals(rstm.get("rs1:100"), Long.valueOf(100L));
        assertEquals(rstm.get("rs2:100"), Long.valueOf(101L));
        assertEquals(rstm.get("rs3:100"), Long.valueOf(103L));
      } else {
        assertEquals(rstm.get("rs1:100"), Long.valueOf(200L));
        assertEquals(rstm.get("rs2:100"), Long.valueOf(201L));
        assertEquals(rstm.get("rs3:100"), Long.valueOf(203L));
      }
    }

    for (TableName t : tables1) {
      Map<String, Long> rstm = result.get(t);
      assertNotNull(rstm);
      assertEquals(rstm.get("rs1:100"), Long.valueOf(200L));
      assertEquals(rstm.get("rs2:100"), Long.valueOf(201L));
      assertEquals(rstm.get("rs3:100"), Long.valueOf(203L));
    }

    cleanBackupTable();

  }

  /**
   * Backup set tests
   */

  @Test
  public void testBackupSetAddNotExists() throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {

      String[] tables = new String[] { "table1", "table2", "table3" };
      String setName = "name";
      table.addToBackupSet(setName, tables);
      List<TableName> tnames = table.describeBackupSet(setName);
      assertTrue(tnames != null);
      assertTrue(tnames.size() == tables.length);
      for (int i = 0; i < tnames.size(); i++) {
        assertTrue(tnames.get(i).getNameAsString().equals(tables[i]));
      }
      cleanBackupTable();
    }

  }

  @Test
  public void testBackupSetAddExists() throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {

      String[] tables = new String[] { "table1", "table2", "table3" };
      String setName = "name";
      table.addToBackupSet(setName, tables);
      String[] addTables = new String[] { "table4", "table5", "table6" };
      table.addToBackupSet(setName, addTables);

      Set<String> expectedTables =
        new HashSet<>(Arrays.asList("table1", "table2", "table3", "table4", "table5", "table6"));

      List<TableName> tnames = table.describeBackupSet(setName);
      assertTrue(tnames != null);
      assertTrue(tnames.size() == expectedTables.size());
      for (TableName tableName : tnames) {
        assertTrue(expectedTables.remove(tableName.getNameAsString()));
      }
      cleanBackupTable();
    }
  }

  @Test
  public void testBackupSetAddExistsIntersects() throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {

      String[] tables = new String[] { "table1", "table2", "table3" };
      String setName = "name";
      table.addToBackupSet(setName, tables);
      String[] addTables = new String[] { "table3", "table4", "table5", "table6" };
      table.addToBackupSet(setName, addTables);

      Set<String> expectedTables =
        new HashSet<>(Arrays.asList("table1", "table2", "table3", "table4", "table5", "table6"));

      List<TableName> tnames = table.describeBackupSet(setName);
      assertTrue(tnames != null);
      assertTrue(tnames.size() == expectedTables.size());
      for (TableName tableName : tnames) {
        assertTrue(expectedTables.remove(tableName.getNameAsString()));
      }
      cleanBackupTable();
    }
  }

  @Test
  public void testBackupSetRemoveSomeNotExists() throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {

      String[] tables = new String[] { "table1", "table2", "table3", "table4" };
      String setName = "name";
      table.addToBackupSet(setName, tables);
      String[] removeTables = new String[] { "table4", "table5", "table6" };
      table.removeFromBackupSet(setName, removeTables);

      Set<String> expectedTables = new HashSet<>(Arrays.asList("table1", "table2", "table3"));

      List<TableName> tnames = table.describeBackupSet(setName);
      assertTrue(tnames != null);
      assertTrue(tnames.size() == expectedTables.size());
      for (TableName tableName : tnames) {
        assertTrue(expectedTables.remove(tableName.getNameAsString()));
      }
      cleanBackupTable();
    }
  }

  @Test
  public void testBackupSetRemove() throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {

      String[] tables = new String[] { "table1", "table2", "table3", "table4" };
      String setName = "name";
      table.addToBackupSet(setName, tables);
      String[] removeTables = new String[] { "table4", "table3" };
      table.removeFromBackupSet(setName, removeTables);

      Set<String> expectedTables = new HashSet<>(Arrays.asList("table1", "table2"));

      List<TableName> tnames = table.describeBackupSet(setName);
      assertTrue(tnames != null);
      assertTrue(tnames.size() == expectedTables.size());
      for (TableName tableName : tnames) {
        assertTrue(expectedTables.remove(tableName.getNameAsString()));
      }
      cleanBackupTable();
    }
  }

  @Test
  public void testBackupSetDelete() throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {

      String[] tables = new String[] { "table1", "table2", "table3", "table4" };
      String setName = "name";
      table.addToBackupSet(setName, tables);
      table.deleteBackupSet(setName);

      List<TableName> tnames = table.describeBackupSet(setName);
      assertTrue(tnames == null);
      cleanBackupTable();
    }
  }

  @Test
  public void testBackupSetList() throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {

      String[] tables = new String[] { "table1", "table2", "table3", "table4" };
      String setName1 = "name1";
      String setName2 = "name2";
      table.addToBackupSet(setName1, tables);
      table.addToBackupSet(setName2, tables);

      List<String> list = table.listBackupSets();

      assertTrue(list.size() == 2);
      assertTrue(list.get(0).equals(setName1));
      assertTrue(list.get(1).equals(setName2));

      cleanBackupTable();
    }
  }

  @Test
  public void testDisallowFurtherIncrementals() throws Exception {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {
      TableName toInvalidate = TableName.valueOf("t1");
      List<TableName> t1 = Lists.newArrayList(toInvalidate, TableName.valueOf("t2"));
      List<TableName> t2 = Lists.newArrayList(toInvalidate, TableName.valueOf("t3"));
      List<TableName> t3 = Lists.newArrayList(TableName.valueOf("t2"), TableName.valueOf("t3"));

      BackupInfo backup = createBackupInfo();
      backup.setState(BackupState.COMPLETE);

      backup.setTables(t1);
      backup.setBackupId("backup1");
      backup.setBackupRootDir("backup1");
      backup.setStartTs(0L);
      table.updateBackupInfo(backup);

      backup.setTables(t2);
      backup.setBackupId("backup2");
      backup.setBackupRootDir("backup2");
      backup.setStartTs(1L);
      table.updateBackupInfo(backup);

      backup.setTables(t3);
      backup.setBackupId("backup3");
      backup.setBackupRootDir("backup2");
      backup.setStartTs(2L);
      table.updateBackupInfo(backup);

      table.disallowFurtherIncrementals(toInvalidate);
      BackupInfo result = table.readBackupInfo("backup1");
      assertTrue(result.isDisallowFurtherIncrementals());

      table.disallowFurtherIncrementals(toInvalidate);
      result = table.readBackupInfo("backup2");
      assertTrue(result.isDisallowFurtherIncrementals());

      table.disallowFurtherIncrementals(toInvalidate);
      result = table.readBackupInfo("backup3");
      assertFalse(result.isDisallowFurtherIncrementals());
    }
  }

  private boolean compare(BackupInfo one, BackupInfo two) {
    return one.getBackupId().equals(two.getBackupId()) && one.getType().equals(two.getType())
      && one.getBackupRootDir().equals(two.getBackupRootDir())
      && one.getStartTs() == two.getStartTs() && one.getCompleteTs() == two.getCompleteTs();
  }

  private BackupInfo createBackupInfo() {
    BackupInfo ctxt = new BackupInfo("backup_" + System.nanoTime(), BackupType.FULL,
      new TableName[] { TableName.valueOf("t1"), TableName.valueOf("t2"), TableName.valueOf("t3") },
      "/hbase/backup");
    ctxt.setStartTs(EnvironmentEdgeManager.currentTime());
    ctxt.setCompleteTs(EnvironmentEdgeManager.currentTime() + 1);
    return ctxt;
  }

  private List<BackupInfo> createBackupInfoList(int size) throws InterruptedException {
    List<BackupInfo> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(createBackupInfo());
      // XXX Why do we need this sleep?
      Thread.sleep(10);
    }
    return list;
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}

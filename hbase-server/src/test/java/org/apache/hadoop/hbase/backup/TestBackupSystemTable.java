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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.backup.BackupHandler.BACKUPSTATUS;
import org.apache.hadoop.hbase.backup.BackupUtil.BackupCompleteData;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test cases for hbase:backup API 
 *
 */
@Category(MediumTests.class)
public class TestBackupSystemTable {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected static Configuration conf = UTIL.getConfiguration();
  protected static MiniHBaseCluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = UTIL.startMiniCluster();

  }

  @Test
  public void testUpdateReadDeleteBackupStatus() throws IOException {
    BackupSystemTable table = BackupSystemTable.getTable(conf);
    BackupContext ctx = createBackupContext();
    table.updateBackupStatus(ctx);
    BackupContext readCtx = table.readBackupStatus(ctx.getBackupId());
    assertTrue(compare(ctx, readCtx));

    // try fake backup id
    readCtx = table.readBackupStatus("fake");

    assertNull(readCtx);
    // delete backup context
    table.deleteBackupStatus(ctx.getBackupId());
    readCtx = table.readBackupStatus(ctx.getBackupId());
    assertNull(readCtx);
    cleanBackupTable();
  }

  @Test
  public void testWriteReadBackupStartCode() throws IOException {
    BackupSystemTable table = BackupSystemTable.getTable(conf);
    String code = "100";
    table.writeBackupStartCode(code);
    String readCode = table.readBackupStartCode();
    assertEquals(code, readCode);
    cleanBackupTable();
  }

  private void cleanBackupTable() throws IOException {
    Admin admin = UTIL.getHBaseAdmin();
    admin.disableTable(BackupSystemTable.getTableName());
    admin.truncateTable(BackupSystemTable.getTableName(), true);
    if (admin.isTableDisabled(BackupSystemTable.getTableName())) {
      admin.enableTable(BackupSystemTable.getTableName());
    }
  }

  @Test
  public void testBackupHistory() throws IOException {
    BackupSystemTable table = BackupSystemTable.getTable(conf);
    int n = 10;
    List<BackupContext> list = createBackupContextList(n);

    // Load data
    for (BackupContext bc : list) {
      // Make sure we set right status
      bc.setFlag(BACKUPSTATUS.COMPLETE);
      table.updateBackupStatus(bc);
    }

    // Reverse list for comparison
    Collections.reverse(list);
    ArrayList<BackupCompleteData> history = table.getBackupHistory();
    assertTrue(history.size() == n);

    for (int i = 0; i < n; i++) {
      BackupContext ctx = list.get(i);
      BackupCompleteData data = history.get(i);
      assertTrue(compare(ctx, data));
    }

    cleanBackupTable();

  }

  @Test
  public void testRegionServerLastLogRollResults() throws IOException {
    BackupSystemTable table = BackupSystemTable.getTable(conf);

    String[] servers = new String[] { "server1", "server2", "server3" };
    String[] timestamps = new String[] { "100", "102", "107" };

    for (int i = 0; i < servers.length; i++) {
      table.writeRegionServerLastLogRollResult(servers[i], timestamps[i]);
    }

    HashMap<String, String> result = table.readRegionServerLastLogRollResult();
    assertTrue(servers.length == result.size());
    Set<String> keys = result.keySet();
    String[] keysAsArray = new String[keys.size()];
    keys.toArray(keysAsArray);
    Arrays.sort(keysAsArray);

    for (int i = 0; i < keysAsArray.length; i++) {
      assertEquals(keysAsArray[i], servers[i]);
      String ts1 = timestamps[i];
      String ts2 = result.get(keysAsArray[i]);
      assertEquals(ts1, ts2);
    }

    cleanBackupTable();

  }

  @Test
  public void testIncrementalBackupTableSet() throws IOException {
    BackupSystemTable table = BackupSystemTable.getTable(conf);

    TreeSet<String> tables1 = new TreeSet<String>();

    tables1.add("t1");
    tables1.add("t2");
    tables1.add("t3");

    TreeSet<String> tables2 = new TreeSet<String>();

    tables2.add("t3");
    tables2.add("t4");
    tables2.add("t5");

    table.addIncrementalBackupTableSet(tables1);
    TreeSet<String> res1 = (TreeSet<String>) table.getIncrementalBackupTableSet();
    assertTrue(tables1.size() == res1.size());
    Iterator<String> desc1 = tables1.descendingIterator();
    Iterator<String> desc2 = res1.descendingIterator();
    while (desc1.hasNext()) {
      assertEquals(desc1.next(), desc2.next());
    }

    table.addIncrementalBackupTableSet(tables2);
    TreeSet<String> res2 = (TreeSet<String>) table.getIncrementalBackupTableSet();
    assertTrue((tables2.size() + tables1.size() - 1) == res2.size());

    tables1.addAll(tables2);

    desc1 = tables1.descendingIterator();
    desc2 = res2.descendingIterator();

    while (desc1.hasNext()) {
      assertEquals(desc1.next(), desc2.next());
    }
    cleanBackupTable();

  }

  @Test
  public void testRegionServerLogTimestampMap() throws IOException {
    BackupSystemTable table = BackupSystemTable.getTable(conf);

    TreeSet<String> tables = new TreeSet<String>();

    tables.add("t1");
    tables.add("t2");
    tables.add("t3");

    HashMap<String, String> rsTimestampMap = new HashMap<String, String>();

    rsTimestampMap.put("rs1", "100");
    rsTimestampMap.put("rs2", "101");
    rsTimestampMap.put("rs3", "103");

    table.writeRegionServerLogTimestamp(tables, rsTimestampMap);

    HashMap<String, HashMap<String, String>> result = table.readLogTimestampMap();

    assertTrue(tables.size() == result.size());

    for (String t : tables) {
      HashMap<String, String> rstm = result.get(t);
      assertNotNull(rstm);
      assertEquals(rstm.get("rs1"), "100");
      assertEquals(rstm.get("rs2"), "101");
      assertEquals(rstm.get("rs3"), "103");
    }

    Set<String> tables1 = new TreeSet<String>();

    tables1.add("t3");
    tables1.add("t4");
    tables1.add("t5");

    HashMap<String, String> rsTimestampMap1 = new HashMap<String, String>();

    rsTimestampMap1.put("rs1", "200");
    rsTimestampMap1.put("rs2", "201");
    rsTimestampMap1.put("rs3", "203");

    table.writeRegionServerLogTimestamp(tables1, rsTimestampMap1);

    result = table.readLogTimestampMap();

    assertTrue(5 == result.size());

    for (String t : tables) {
      HashMap<String, String> rstm = result.get(t);
      assertNotNull(rstm);
      if (t.equals("t3") == false) {
        assertEquals(rstm.get("rs1"), "100");
        assertEquals(rstm.get("rs2"), "101");
        assertEquals(rstm.get("rs3"), "103");
      } else {
        assertEquals(rstm.get("rs1"), "200");
        assertEquals(rstm.get("rs2"), "201");
        assertEquals(rstm.get("rs3"), "203");
      }
    }

    for (String t : tables1) {
      HashMap<String, String> rstm = result.get(t);
      assertNotNull(rstm);
      assertEquals(rstm.get("rs1"), "200");
      assertEquals(rstm.get("rs2"), "201");
      assertEquals(rstm.get("rs3"), "203");
    }

    cleanBackupTable();

  }

  @Test
  public void testAddWALFiles() throws IOException {
    BackupSystemTable table = BackupSystemTable.getTable(conf);
    FileSystem fs = FileSystem.get(conf);
    List<String> files =
        Arrays.asList("hdfs://server/WALs/srv1,101,15555/srv1,101,15555.default.1",
          "hdfs://server/WALs/srv2,102,16666/srv2,102,16666.default.2",
            "hdfs://server/WALs/srv3,103,17777/srv3,103,17777.default.3");
    String newFile = "hdfs://server/WALs/srv1,101,15555/srv1,101,15555.default.5";

    table.addWALFiles(files, "backup");

    assertTrue(table.checkWALFile(files.get(0)));
    assertTrue(table.checkWALFile(files.get(1)));
    assertTrue(table.checkWALFile(files.get(2)));
    assertFalse(table.checkWALFile(newFile));

    cleanBackupTable();
  }

  private boolean compare(BackupContext ctx, BackupCompleteData data) {

    return ctx.getBackupId().equals(data.getBackupToken())
        && ctx.getTargetRootDir().equals(data.getBackupRootPath())
        && ctx.getType().equals(data.getType())
        && ctx.getStartTs() == Long.parseLong(data.getStartTime())
        && ctx.getEndTs() == Long.parseLong(data.getEndTime());

  }

  private boolean compare(BackupContext one, BackupContext two) {
    return one.getBackupId().equals(two.getBackupId()) && one.getType().equals(two.getType())
        && one.getTargetRootDir().equals(two.getTargetRootDir())
        && one.getStartTs() == two.getStartTs() && one.getEndTs() == two.getEndTs();
  }

  private BackupContext createBackupContext() {

    BackupContext ctxt =
        new BackupContext("backup_" + System.nanoTime(), "full", new String[] { "t1", "t2", "t3" },
          "/hbase/backup", null);
    ctxt.setStartTs(System.currentTimeMillis());
    ctxt.setEndTs(System.currentTimeMillis() + 1);
    return ctxt;
  }

  private List<BackupContext> createBackupContextList(int size) {
    List<BackupContext> list = new ArrayList<BackupContext>();
    for (int i = 0; i < size; i++) {
      list.add(createBackupContext());
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return list;
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) cluster.shutdown();
  }

}

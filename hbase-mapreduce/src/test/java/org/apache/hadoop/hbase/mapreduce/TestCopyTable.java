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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Basic test for the CopyTable M/R tool
 */
@Category({ MapReduceTests.class, LargeTests.class })
public class TestCopyTable extends CopyTableTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCopyTable.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Override
  protected Table createSourceTable(TableDescriptor desc) throws Exception {
    return TEST_UTIL.createTable(desc, null);
  }

  @Override
  protected Table createTargetTable(TableDescriptor desc) throws Exception {
    return TEST_UTIL.createTable(desc, null);
  }

  @Override
  protected void dropSourceTable(TableName tableName) throws Exception {
    TEST_UTIL.deleteTable(tableName);
  }

  @Override
  protected void dropTargetTable(TableName tableName) throws Exception {
    TEST_UTIL.deleteTable(tableName);
  }

  @Override
  protected String[] getPeerClusterOptions() throws Exception {
    return new String[0];
  }

  /**
   * Simple end-to-end test
   */
  @Test
  public void testCopyTable() throws Exception {
    doCopyTableTest(TEST_UTIL.getConfiguration(), false);
  }

  /**
   * Simple end-to-end test with bulkload.
   */
  @Test
  public void testCopyTableWithBulkload() throws Exception {
    doCopyTableTest(TEST_UTIL.getConfiguration(), true);
  }

  /**
   * Simple end-to-end test on table with MOB
   */
  @Test
  public void testCopyTableWithMob() throws Exception {
    doCopyTableTestWithMob(TEST_UTIL.getConfiguration(), false);
  }

  /**
   * Simple end-to-end test with bulkload on table with MOB.
   */
  @Test
  public void testCopyTableWithBulkloadWithMob() throws Exception {
    doCopyTableTestWithMob(TEST_UTIL.getConfiguration(), true);
  }

  @Test
  public void testStartStopRow() throws Exception {
    testStartStopRow(TEST_UTIL.getConfiguration());
  }

  /**
   * Test copy of table from sourceTable to targetTable all rows from family a
   */
  @Test
  public void testRenameFamily() throws Exception {
    testRenameFamily(TEST_UTIL.getConfiguration());
  }

  /**
   * Test main method of CopyTable.
   */
  @Test
  public void testMainMethod() throws Exception {
    String[] emptyArgs = { "-h" };
    PrintStream oldWriter = System.err;
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintStream writer = new PrintStream(data);
    System.setErr(writer);
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager = new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    try {
      CopyTable.main(emptyArgs);
      fail("should be exit");
    } catch (SecurityException e) {
      assertEquals(1, newSecurityManager.getExitCode());
    } finally {
      System.setErr(oldWriter);
      System.setSecurityManager(SECURITY_MANAGER);
    }
    assertTrue(data.toString().contains("rs.class"));
    // should print usage information
    assertTrue(data.toString().contains("Usage:"));
  }

  private Table createTable(TableName tableName, byte[] family, boolean isMob) throws IOException {
    if (isMob) {
      ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setMobEnabled(true).setMobThreshold(1).build();
      TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cfd).build();
      return TEST_UTIL.createTable(desc, null);
    } else {
      return TEST_UTIL.createTable(tableName, family);
    }
  }

  private void testCopyTableBySnapshot(String tablePrefix, boolean bulkLoad, boolean isMob)
    throws Exception {
    TableName table1 = TableName.valueOf(tablePrefix + 1);
    TableName table2 = TableName.valueOf(tablePrefix + 2);
    String snapshot = tablePrefix + "_snapshot";
    try (Table t1 = createTable(table1, FAMILY_A, isMob);
      Table t2 = createTable(table2, FAMILY_A, isMob)) {
      loadData(t1, FAMILY_A, Bytes.toBytes("qualifier"));
      TEST_UTIL.getAdmin().snapshot(snapshot, table1);
      boolean success;
      if (bulkLoad) {
        success = runCopy(TEST_UTIL.getConfiguration(),
          new String[] { "--snapshot", "--new.name=" + table2, "--bulkload", snapshot });
      } else {
        success = runCopy(TEST_UTIL.getConfiguration(),
          new String[] { "--snapshot", "--new.name=" + table2, snapshot });
      }
      assertTrue(success);
      verifyRows(t2, FAMILY_A, Bytes.toBytes("qualifier"));
    } finally {
      TEST_UTIL.getAdmin().deleteSnapshot(snapshot);
      TEST_UTIL.deleteTable(table1);
      TEST_UTIL.deleteTable(table2);
    }
  }

  @Test
  public void testLoadingSnapshotToTable() throws Exception {
    testCopyTableBySnapshot("testLoadingSnapshotToTable", false, false);
  }

  @Test
  public void testLoadingTtlExpiredSnapshotToTable() throws Exception {
    String tablePrefix = "testLoadingExpiredSnapshotToTable";
    TableName table1 = TableName.valueOf(tablePrefix + 1);
    TableName table2 = TableName.valueOf(tablePrefix + 2);
    Table t1 = createTable(table1, FAMILY_A, false);
    createTable(table2, FAMILY_A, false);
    loadData(t1, FAMILY_A, Bytes.toBytes("qualifier"));
    String snapshot = tablePrefix + "_snapshot";
    Map<String, Object> properties = new HashMap<>();
    properties.put("TTL", 10);
    SnapshotDescription snapshotDescription = new SnapshotDescription(snapshot, table1,
      SnapshotType.FLUSH, null, EnvironmentEdgeManager.currentTime(), -1, properties);
    TEST_UTIL.getAdmin().snapshot(snapshotDescription);
    boolean isExist =
      TEST_UTIL.getAdmin().listSnapshots().stream().anyMatch(ele -> snapshot.equals(ele.getName()));
    assertTrue(isExist);
    int retry = 6;
    while (
      !SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDescription.getTtl(),
        snapshotDescription.getCreationTime(), EnvironmentEdgeManager.currentTime()) && retry > 0
    ) {
      retry--;
      Thread.sleep(10 * 1000);
    }
    boolean isExpiredSnapshot =
      SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDescription.getTtl(),
        snapshotDescription.getCreationTime(), EnvironmentEdgeManager.currentTime());
    assertTrue(isExpiredSnapshot);
    String[] args = new String[] { "--snapshot", "--new.name=" + table2, "--bulkload", snapshot };
    assertThrows(SnapshotTTLExpiredException.class,
      () -> runCopy(TEST_UTIL.getConfiguration(), args));
  }

  @Test
  public void tsetLoadingSnapshotToMobTable() throws Exception {
    testCopyTableBySnapshot("testLoadingSnapshotToMobTable", false, true);
  }

  @Test
  public void testLoadingSnapshotAndBulkLoadToTable() throws Exception {
    testCopyTableBySnapshot("testLoadingSnapshotAndBulkLoadToTable", true, false);
  }

  @Test
  public void testLoadingSnapshotAndBulkLoadToMobTable() throws Exception {
    testCopyTableBySnapshot("testLoadingSnapshotAndBulkLoadToMobTable", true, true);
  }

  @Test
  public void testLoadingSnapshotWithoutSnapshotName() throws Exception {
    assertFalse(runCopy(TEST_UTIL.getConfiguration(), new String[] { "--snapshot" }));
  }

  @Test
  public void testLoadingSnapshotWithoutDestTable() throws Exception {
    assertFalse(
      runCopy(TEST_UTIL.getConfiguration(), new String[] { "--snapshot", "sourceSnapshotName" }));
  }

}

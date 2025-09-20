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
package org.apache.hadoop.hbase.mob;

import static org.apache.hadoop.hbase.mob.MobConstants.MOB_CLEANER_BATCH_SIZE_UPPER_BOUND;
import static org.apache.hadoop.hbase.mob.MobConstants.MOB_CLEANER_THREAD_COUNT;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, MasterTests.class })
public class TestExpiredMobFileCleanerChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestExpiredMobFileCleanerChore.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private final static TableName tableName = TableName.valueOf("TestExpiredMobFileCleaner");
  private final static TableName tableName2 = TableName.valueOf("TestExpiredMobFileCleaner2");
  private final static String family = "family";
  private final static byte[] row1 = Bytes.toBytes("row1");
  private final static byte[] row2 = Bytes.toBytes("row2");
  private final static byte[] row3 = Bytes.toBytes("row3");
  private final static byte[] qf = Bytes.toBytes("qf");

  private static BufferedMutator table;
  private static Admin admin;
  private static BufferedMutator table2;
  private static MobFileCleanerChore mobFileCleanerChore;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    TEST_UTIL.getConfiguration().setInt(MOB_CLEANER_BATCH_SIZE_UPPER_BOUND, 2);
    TEST_UTIL.startMiniCluster(1);
    mobFileCleanerChore = TEST_UTIL.getMiniHBaseCluster().getMaster().getMobFileCleanerChore();
  }

  @After
  public void cleanUp() throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.disableTable(tableName2);
    admin.deleteTable(tableName2);
    admin.close();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testCleanerSingleThread() throws Exception {
    TEST_UTIL.getConfiguration().setInt(MOB_CLEANER_THREAD_COUNT, 1);
    mobFileCleanerChore.onConfigurationChange(TEST_UTIL.getConfiguration());
    int corePoolSize = mobFileCleanerChore.getExecutor().getCorePoolSize();
    Assert.assertEquals(1, corePoolSize);
    testCleanerInternal();
  }

  @Test
  public void testCleanerMultiThread() throws Exception {
    TEST_UTIL.getConfiguration().setInt(MOB_CLEANER_THREAD_COUNT, 2);
    mobFileCleanerChore.onConfigurationChange(TEST_UTIL.getConfiguration());
    int corePoolSize = mobFileCleanerChore.getExecutor().getCorePoolSize();
    Assert.assertEquals(2, corePoolSize);
    testCleanerInternal();
  }

  private static void init() throws Exception {
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).setMobEnabled(true)
        .setMobThreshold(3L).setMaxVersions(4).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

    admin = TEST_UTIL.getAdmin();
    admin.createTable(tableDescriptorBuilder.build());

    table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())
      .getBufferedMutator(tableName);

    TableDescriptorBuilder tableDescriptorBuilder2 = TableDescriptorBuilder.newBuilder(tableName2);
    ColumnFamilyDescriptor columnFamilyDescriptor2 =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).setMobEnabled(true)
        .setMobThreshold(3L).setMaxVersions(4).build();
    tableDescriptorBuilder2.setColumnFamily(columnFamilyDescriptor2);
    admin.createTable(tableDescriptorBuilder2.build());

    table2 = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())
      .getBufferedMutator(tableName2);
  }

  private static void modifyColumnExpiryDays(int expireDays) throws Exception {

    // change ttl as expire days to make some row expired
    int timeToLive = expireDays * secondsOfDay();
    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes(family)).setMobEnabled(true).setMobThreshold(3L);
    columnFamilyDescriptorBuilder.setTimeToLive(timeToLive);

    admin.modifyColumnFamily(tableName, columnFamilyDescriptorBuilder.build());

    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder2 = ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes(family)).setMobEnabled(true).setMobThreshold(3L);
    columnFamilyDescriptorBuilder2.setTimeToLive(timeToLive);

    admin.modifyColumnFamily(tableName2, columnFamilyDescriptorBuilder2.build());
  }

  private static void putKVAndFlush(BufferedMutator table, byte[] row, byte[] value, long ts,
    TableName tableName) throws Exception {

    Put put = new Put(row, ts);
    put.addColumn(Bytes.toBytes(family), qf, value);
    table.mutate(put);

    table.flush();
    admin.flush(tableName);
  }

  /**
   * Creates a 3 day old hfile and an 1 day old hfile then sets expiry to 2 days. Verifies that the
   * 3 day old hfile is removed but the 1 day one is still present after the expiry based cleaner is
   * run.
   */

  public static void testCleanerInternal() throws Exception {
    init();

    Path mobDirPath = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);

    byte[] dummyData = makeDummyData(600);
    long ts = EnvironmentEdgeManager.currentTime() - 3 * secondsOfDay() * 1000; // 3 days before
    putKVAndFlush(table, row1, dummyData, ts, tableName);
    FileStatus[] firstFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    // the first mob file
    assertEquals("Before cleanup without delay 1", 1, firstFiles.length);
    String firstFile = firstFiles[0].getPath().getName();

    // 1.5 day before
    ts = (long) (EnvironmentEdgeManager.currentTime() - 1.5 * secondsOfDay() * 1000);
    putKVAndFlush(table, row2, dummyData, ts, tableName);
    FileStatus[] secondFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    // now there are 2 mob files
    assertEquals("Before cleanup without delay 2", 2, secondFiles.length);
    String f1 = secondFiles[0].getPath().getName();
    String f2 = secondFiles[1].getPath().getName();
    String secondFile = f1.equals(firstFile) ? f2 : f1;

    ts = EnvironmentEdgeManager.currentTime() - 4 * secondsOfDay() * 1000; // 4 days before
    putKVAndFlush(table, row3, dummyData, ts, tableName);
    ts = EnvironmentEdgeManager.currentTime() - 4 * secondsOfDay() * 1000; // 4 days before
    putKVAndFlush(table, row3, dummyData, ts, tableName);
    FileStatus[] thirdFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    // now there are 4 mob files
    assertEquals("Before cleanup without delay 3", 4, thirdFiles.length);

    // modifyColumnExpiryDays(2); // ttl = 2, make the first row expired

    // for table 2
    Path mobDirPath2 = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName2, family);

    byte[] dummyData2 = makeDummyData(600);

    putKVAndFlush(table2, row1, dummyData2, ts, tableName2);
    FileStatus[] firstFiles2 = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath2);
    // the first mob file
    assertEquals("Before cleanup without delay 1", 1, firstFiles2.length);
    String firstFile2 = firstFiles2[0].getPath().getName();

    // 1.5 day before
    ts = (long) (EnvironmentEdgeManager.currentTime() - 1.5 * secondsOfDay() * 1000);
    putKVAndFlush(table2, row2, dummyData2, ts, tableName2);
    FileStatus[] secondFiles2 = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath2);
    // now there are 2 mob files
    assertEquals("Before cleanup without delay 2", 2, secondFiles2.length);
    String f1Second = secondFiles2[0].getPath().getName();
    String f2Second = secondFiles2[1].getPath().getName();
    String secondFile2 = f1Second.equals(firstFile2) ? f2Second : f1Second;
    ts = EnvironmentEdgeManager.currentTime() - 4 * secondsOfDay() * 1000; // 4 days before
    putKVAndFlush(table2, row3, dummyData2, ts, tableName2);
    ts = EnvironmentEdgeManager.currentTime() - 4 * secondsOfDay() * 1000; // 4 days before
    putKVAndFlush(table2, row3, dummyData2, ts, tableName2);
    FileStatus[] thirdFiles2 = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath2);
    // now there are 4 mob files
    assertEquals("Before cleanup without delay 3", 4, thirdFiles2.length);

    modifyColumnExpiryDays(2); // ttl = 2, make the first row expired

    // run the cleaner chore
    mobFileCleanerChore.chore();

    FileStatus[] filesAfterClean = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    String lastFile = filesAfterClean[0].getPath().getName();
    // there are 4 mob files in total, but only 3 need to be cleaned
    assertEquals("After cleanup without delay 1", 1, filesAfterClean.length);
    assertEquals("After cleanup without delay 2", secondFile, lastFile);

    filesAfterClean = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath2);
    lastFile = filesAfterClean[0].getPath().getName();
    // there are 4 mob files in total, but only 3 need to be cleaned
    assertEquals("After cleanup without delay 1", 1, filesAfterClean.length);
    assertEquals("After cleanup without delay 2", secondFile2, lastFile);
  }

  private static int secondsOfDay() {
    return 24 * 3600;
  }

  private static byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    Bytes.random(dummyData);
    return dummyData;
  }
}

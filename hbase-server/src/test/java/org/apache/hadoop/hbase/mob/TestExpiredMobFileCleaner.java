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
import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestExpiredMobFileCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestExpiredMobFileCleaner.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private final static TableName tableName = TableName.valueOf("TestExpiredMobFileCleaner");
  private final static String family = "family";
  private final static byte[] row1 = Bytes.toBytes("row1");
  private final static byte[] row2 = Bytes.toBytes("row2");
  private final static byte[] row3 = Bytes.toBytes("row3");
  private final static byte[] qf = Bytes.toBytes("qf");
  private static final Logger LOG = LoggerFactory.getLogger(TestExpiredMobFileCleaner.class);

  private static BufferedMutator table;
  private static Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    TEST_UTIL.getConfiguration().setInt(MOB_CLEANER_BATCH_SIZE_UPPER_BOUND, 2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  private void init() throws Exception {
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).setMobEnabled(true)
        .setMobThreshold(3L).setMaxVersions(4).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

    admin = TEST_UTIL.getAdmin();
    admin.createTable(tableDescriptorBuilder.build());
    table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())
      .getBufferedMutator(tableName);
  }

  private void modifyColumnExpiryDays(int expireDays) throws Exception {
    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes(family)).setMobEnabled(true).setMobThreshold(3L);
    // change ttl as expire days to make some row expired
    int timeToLive = expireDays * secondsOfDay();
    columnFamilyDescriptorBuilder.setTimeToLive(timeToLive);

    admin.modifyColumnFamily(tableName, columnFamilyDescriptorBuilder.build());
  }

  private void putKVAndFlush(BufferedMutator table, byte[] row, byte[] value, long ts)
    throws Exception {

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
  @Test
  public void testCleaner() throws Exception {
    init();

    Path mobDirPath = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);

    byte[] dummyData = makeDummyData(600);
    long ts = EnvironmentEdgeManager.currentTime() - 3 * secondsOfDay() * 1000; // 3 days before
    putKVAndFlush(table, row1, dummyData, ts);
    LOG.info("test log to be deleted, tablename is " + tableName);
    CommonFSUtils.logFileSystemState(TEST_UTIL.getTestFileSystem(),
      TEST_UTIL.getDefaultRootDirPath(), LOG);
    FileStatus[] firstFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    // the first mob file
    assertEquals("Before cleanup without delay 1", 1, firstFiles.length);
    String firstFile = firstFiles[0].getPath().getName();

    // 1.5 day before
    ts = (long) (EnvironmentEdgeManager.currentTime() - 1.5 * secondsOfDay() * 1000);
    putKVAndFlush(table, row2, dummyData, ts);
    FileStatus[] secondFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    // now there are 2 mob files
    assertEquals("Before cleanup without delay 2", 2, secondFiles.length);
    String f1 = secondFiles[0].getPath().getName();
    String f2 = secondFiles[1].getPath().getName();
    String secondFile = f1.equals(firstFile) ? f2 : f1;

    ts = EnvironmentEdgeManager.currentTime() - 4 * secondsOfDay() * 1000; // 4 days before
    putKVAndFlush(table, row3, dummyData, ts);
    ts = EnvironmentEdgeManager.currentTime() - 4 * secondsOfDay() * 1000; // 4 days before
    putKVAndFlush(table, row3, dummyData, ts);
    FileStatus[] thirdFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    // now there are 4 mob files
    assertEquals("Before cleanup without delay 3", 4, thirdFiles.length);

    modifyColumnExpiryDays(2); // ttl = 2, make the first row expired

    // run the cleaner
    String[] args = new String[2];
    args[0] = tableName.getNameAsString();
    args[1] = family;
    ToolRunner.run(TEST_UTIL.getConfiguration(), new ExpiredMobFileCleaner(), args);

    FileStatus[] filesAfterClean = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    String lastFile = filesAfterClean[0].getPath().getName();
    // there are 4 mob files in total, but only 3 need to be cleaned
    assertEquals("After cleanup without delay 1", 1, filesAfterClean.length);
    assertEquals("After cleanup without delay 2", secondFile, lastFile);
  }

  private int secondsOfDay() {
    return 24 * 3600;
  }

  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    Bytes.random(dummyData);
    return dummyData;
  }
}

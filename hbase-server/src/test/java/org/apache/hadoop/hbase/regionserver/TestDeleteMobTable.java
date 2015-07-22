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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestDeleteMobTable {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte[] FAMILY = Bytes.toBytes("family");
  private final static byte[] QF = Bytes.toBytes("qualifier");
  private static Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Generate the mob value.
   *
   * @param size
   *          the size of the value
   * @return the mob value generated
   */
  private static byte[] generateMobValue(int size) {
    byte[] mobVal = new byte[size];
    random.nextBytes(mobVal);
    return mobVal;
  }

  @Test
  public void testDeleteMobTable() throws Exception {
    byte[] tableName = Bytes.toBytes("testDeleteMobTable");
    TableName tn = TableName.valueOf(tableName);
    HTableDescriptor htd = new HTableDescriptor(tn);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(0);
    htd.addFamily(hcd);
    HBaseAdmin admin = null;
    Table table = null;
    try {
      admin = TEST_UTIL.getHBaseAdmin();
      admin.createTable(htd);
      table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration()).getTable(tn);
      byte[] value = generateMobValue(10);

      byte[] row = Bytes.toBytes("row");
      Put put = new Put(row);
      put.addColumn(FAMILY, QF, EnvironmentEdgeManager.currentTime(), value);
      table.put(put);

      admin.flush(tn);

      // the mob file exists
      Assert.assertEquals(1, countMobFiles(tn, hcd.getNameAsString()));
      Assert.assertEquals(0, countArchiveMobFiles(tn, hcd.getNameAsString()));
      String fileName = assertHasOneMobRow(table, tn, hcd.getNameAsString());
      Assert.assertFalse(mobArchiveExist(tn, hcd.getNameAsString(), fileName));
      Assert.assertTrue(mobTableDirExist(tn));
      table.close();

      admin.disableTable(tn);
      admin.deleteTable(tn);

      Assert.assertFalse(admin.tableExists(tn));
      Assert.assertEquals(0, countMobFiles(tn, hcd.getNameAsString()));
      Assert.assertEquals(1, countArchiveMobFiles(tn, hcd.getNameAsString()));
      Assert.assertTrue(mobArchiveExist(tn, hcd.getNameAsString(), fileName));
      Assert.assertFalse(mobTableDirExist(tn));
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  @Test
  public void testDeleteNonMobTable() throws Exception {
    byte[] tableName = Bytes.toBytes("testDeleteNonMobTable");
    TableName tn = TableName.valueOf(tableName);
    HTableDescriptor htd = new HTableDescriptor(tn);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    htd.addFamily(hcd);
    HBaseAdmin admin = null;
    Table table = null;
    try {
      admin = TEST_UTIL.getHBaseAdmin();
      admin.createTable(htd);
      table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration()).getTable(tn);
      byte[] value = generateMobValue(10);

      byte[] row = Bytes.toBytes("row");
      Put put = new Put(row);
      put.addColumn(FAMILY, QF, EnvironmentEdgeManager.currentTime(), value);
      table.put(put);

      admin.flush(tn);
      table.close();

      // the mob file doesn't exist
      Assert.assertEquals(0, countMobFiles(tn, hcd.getNameAsString()));
      Assert.assertEquals(0, countArchiveMobFiles(tn, hcd.getNameAsString()));
      Assert.assertFalse(mobTableDirExist(tn));

      admin.disableTable(tn);
      admin.deleteTable(tn);

      Assert.assertFalse(admin.tableExists(tn));
      Assert.assertEquals(0, countMobFiles(tn, hcd.getNameAsString()));
      Assert.assertEquals(0, countArchiveMobFiles(tn, hcd.getNameAsString()));
      Assert.assertFalse(mobTableDirExist(tn));
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  @Test
  public void testMobFamilyDelete() throws Exception {
    byte[] tableName = Bytes.toBytes("testMobFamilyDelete");
    TableName tn = TableName.valueOf(tableName);
    HTableDescriptor htd = new HTableDescriptor(tn);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(0);
    htd.addFamily(hcd);
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("family2")));
    HBaseAdmin admin = null;
    Table table = null;
    try {
      admin = TEST_UTIL.getHBaseAdmin();
      admin.createTable(htd);
      table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration()).getTable(tn);
      byte[] value = generateMobValue(10);
      byte[] row = Bytes.toBytes("row");
      Put put = new Put(row);
      put.addColumn(FAMILY, QF, EnvironmentEdgeManager.currentTime(), value);
      table.put(put);
      admin.flush(tn);
      // the mob file exists
      Assert.assertEquals(1, countMobFiles(tn, hcd.getNameAsString()));
      Assert.assertEquals(0, countArchiveMobFiles(tn, hcd.getNameAsString()));
      String fileName = assertHasOneMobRow(table, tn, hcd.getNameAsString());
      Assert.assertFalse(mobArchiveExist(tn, hcd.getNameAsString(), fileName));
      Assert.assertTrue(mobTableDirExist(tn));
      admin.deleteColumnFamily(tn, FAMILY);
      Assert.assertEquals(0, countMobFiles(tn, hcd.getNameAsString()));
      Assert.assertEquals(1, countArchiveMobFiles(tn, hcd.getNameAsString()));
      Assert.assertTrue(mobArchiveExist(tn, hcd.getNameAsString(), fileName));
      Assert.assertFalse(mobColumnFamilyDirExist(tn));
    } finally {
      table.close();
      if (admin != null) {
        admin.close();
      }
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private int countMobFiles(TableName tn, String familyName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path mobFileDir = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName);
    if (fs.exists(mobFileDir)) {
      return fs.listStatus(mobFileDir).length;
    } else {
      return 0;
    }
  }

  private int countArchiveMobFiles(TableName tn, String familyName)
      throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path storePath = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(), tn,
        MobUtils.getMobRegionInfo(tn).getEncodedName(), familyName);
    if (fs.exists(storePath)) {
      return fs.listStatus(storePath).length;
    } else {
      return 0;
    }
  }

  private boolean mobTableDirExist(TableName tn) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path tableDir = FSUtils.getTableDir(MobUtils.getMobHome(TEST_UTIL.getConfiguration()), tn);
    return fs.exists(tableDir);
  }

  private boolean mobColumnFamilyDirExist(TableName tn) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path tableDir = FSUtils.getTableDir(MobUtils.getMobHome(TEST_UTIL.getConfiguration()), tn);
    HRegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(tn);
    Path mobFamilyDir = new Path(tableDir, new Path(mobRegionInfo.getEncodedName(),
      Bytes.toString(FAMILY)));
    return fs.exists(mobFamilyDir);
  }

  private boolean mobArchiveExist(TableName tn, String familyName, String fileName)
      throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path storePath = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(), tn,
        MobUtils.getMobRegionInfo(tn).getEncodedName(), familyName);
    return fs.exists(new Path(storePath, fileName));
  }

  private String assertHasOneMobRow(Table table, TableName tn, String familyName)
      throws IOException {
    Scan scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner rs = table.getScanner(scan);
    Result r = rs.next();
    Assert.assertNotNull(r);
    byte[] value = r.getValue(FAMILY, QF);
    String fileName = Bytes.toString(value, Bytes.SIZEOF_INT, value.length - Bytes.SIZEOF_INT);
    Path filePath = new Path(
        MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName), fileName);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Assert.assertTrue(fs.exists(filePath));
    r = rs.next();
    Assert.assertNull(r);
    return fileName;
  }
}

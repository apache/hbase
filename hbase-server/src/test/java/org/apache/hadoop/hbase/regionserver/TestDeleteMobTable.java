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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestDeleteMobTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDeleteMobTable.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte[] FAMILY = Bytes.toBytes("family");
  private final static byte[] QF = Bytes.toBytes("qualifier");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
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
    Bytes.random(mobVal);
    return mobVal;
  }

  private HTableDescriptor createTableDescriptor(TableName tableName, boolean hasMob) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    if (hasMob) {
      hcd.setMobEnabled(true);
      hcd.setMobThreshold(0);
    }
    htd.addFamily(hcd);
    return htd;
  }

  private Table createTableWithOneFile(HTableDescriptor htd) throws IOException {
    Table table = TEST_UTIL.createTable(htd, null);
    try {
      // insert data
      byte[] value = generateMobValue(10);
      byte[] row = Bytes.toBytes("row");
      Put put = new Put(row);
      put.addColumn(FAMILY, QF, EnvironmentEdgeManager.currentTime(), value);
      table.put(put);

      // create an hfile
      TEST_UTIL.getAdmin().flush(htd.getTableName());
    } catch (IOException e) {
      table.close();
      throw e;
    }
    return table;
  }

  @Test
  public void testDeleteMobTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = createTableDescriptor(tableName, true);
    HColumnDescriptor hcd = htd.getFamily(FAMILY);

    String fileName = null;
    Table table = createTableWithOneFile(htd);
    try {
      // the mob file exists
      Assert.assertEquals(1, countMobFiles(tableName, hcd.getNameAsString()));
      Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd.getNameAsString()));
      fileName = assertHasOneMobRow(table, tableName, hcd.getNameAsString());
      Assert.assertFalse(mobArchiveExist(tableName, hcd.getNameAsString(), fileName));
      Assert.assertTrue(mobTableDirExist(tableName));
    } finally {
      table.close();
      TEST_UTIL.deleteTable(tableName);
    }

    Assert.assertFalse(TEST_UTIL.getAdmin().tableExists(tableName));
    Assert.assertEquals(0, countMobFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(1, countArchiveMobFiles(tableName, hcd.getNameAsString()));
    Assert.assertTrue(mobArchiveExist(tableName, hcd.getNameAsString(), fileName));
    Assert.assertFalse(mobTableDirExist(tableName));
  }

  @Test
  public void testDeleteNonMobTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = createTableDescriptor(tableName, false);
    HColumnDescriptor hcd = htd.getFamily(FAMILY);

    Table table = createTableWithOneFile(htd);
    try {
      // the mob file doesn't exist
      Assert.assertEquals(0, countMobFiles(tableName, hcd.getNameAsString()));
      Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd.getNameAsString()));
      Assert.assertFalse(mobTableDirExist(tableName));
    } finally {
      table.close();
      TEST_UTIL.deleteTable(tableName);
    }

    Assert.assertFalse(TEST_UTIL.getAdmin().tableExists(tableName));
    Assert.assertEquals(0, countMobFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd.getNameAsString()));
    Assert.assertFalse(mobTableDirExist(tableName));
  }

  @Test
  public void testMobFamilyDelete() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = createTableDescriptor(tableName, true);
    HColumnDescriptor hcd = htd.getFamily(FAMILY);
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("family2")));

    Table table = createTableWithOneFile(htd);
    try {
      // the mob file exists
      Assert.assertEquals(1, countMobFiles(tableName, hcd.getNameAsString()));
      Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd.getNameAsString()));
      String fileName = assertHasOneMobRow(table, tableName, hcd.getNameAsString());
      Assert.assertFalse(mobArchiveExist(tableName, hcd.getNameAsString(), fileName));
      Assert.assertTrue(mobTableDirExist(tableName));

      TEST_UTIL.getAdmin().deleteColumnFamily(tableName, FAMILY);

      Assert.assertEquals(0, countMobFiles(tableName, hcd.getNameAsString()));
      Assert.assertEquals(1, countArchiveMobFiles(tableName, hcd.getNameAsString()));
      Assert.assertTrue(mobArchiveExist(tableName, hcd.getNameAsString(), fileName));
      Assert.assertFalse(mobColumnFamilyDirExist(tableName, hcd.getNameAsString()));
    } finally {
      table.close();
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private int countMobFiles(TableName tn, String familyName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path mobFileDir = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName);
    if (fs.exists(mobFileDir)) {
      return fs.listStatus(mobFileDir).length;
    }
    return 0;
  }

  private int countArchiveMobFiles(TableName tn, String familyName)
      throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path storePath = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(), tn,
        MobUtils.getMobRegionInfo(tn).getEncodedName(), familyName);
    if (fs.exists(storePath)) {
      return fs.listStatus(storePath).length;
    }
    return 0;
  }

  private boolean mobTableDirExist(TableName tn) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path tableDir =
      CommonFSUtils.getTableDir(MobUtils.getMobHome(TEST_UTIL.getConfiguration()), tn);
    return fs.exists(tableDir);
  }

  private boolean mobColumnFamilyDirExist(TableName tn, String familyName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path mobFamilyDir = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName);
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
    String fileName = MobUtils.getMobFileName(r.getColumnLatestCell(FAMILY, QF));
    Path filePath = new Path(
        MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, familyName), fileName);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Assert.assertTrue(fs.exists(filePath));
    r = rs.next();
    Assert.assertNull(r);
    return fileName;
  }
}

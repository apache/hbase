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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestDeleteTable{

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDeleteTable.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] FAMILY1 = Bytes.toBytes("normal");
  private final static byte[] FAMILY2 = Bytes.toBytes("mob");

  private final static byte[] QF = Bytes.toBytes("qualifier");
  private static Random random = new Random();

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


  private static byte[] generateValue(int size) {
    byte[] val = new byte[size];
    random.nextBytes(val);
    return val;
  }

  private HTableDescriptor createTableDescriptor(TableName tableName) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd1 = new HColumnDescriptor(FAMILY1);
    HColumnDescriptor hcd2 = new HColumnDescriptor(FAMILY2);

    hcd1.setMobEnabled(true);
    hcd1.setMobThreshold(0);


    htd.addFamily(hcd1);
    htd.addFamily(hcd2);
    return htd;
  }

  private Table createTableWithFiles(HTableDescriptor htd) throws IOException {
    Table table = TEST_UTIL.createTable(htd, null);
    try {
      // insert data
      byte[] value = generateValue(10);
      byte[] row = Bytes.toBytes("row");
      Put put1 = new Put(row);
      put1.addColumn(FAMILY1, QF, EnvironmentEdgeManager.currentTime(), value);
      table.put(put1);

      Put put2 = new Put(row);
      put2.addColumn(FAMILY2, QF, EnvironmentEdgeManager.currentTime(), value);
      table.put(put2);

      // create an hfile
      TEST_UTIL.getAdmin().flush(htd.getTableName());
    } catch (IOException e) {
      table.close();
      throw e;
    }
    return table;
  }


  @Test
  public void testDeleteTableWithoutArchive() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = createTableDescriptor(tableName);
    HColumnDescriptor hcd_mob = htd.getFamily(FAMILY1);
    HColumnDescriptor hcd = htd.getFamily(FAMILY2);

    Table table = createTableWithFiles(htd);

    HRegionInfo regionInfo = TEST_UTIL.getHBaseAdmin().getTableRegions(tableName).get(0);

    // the mob file exists
    Assert.assertEquals(1, countMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(1, countRegionFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(0, countArchiveRegionFiles(tableName, hcd.getNameAsString(), regionInfo));
    Assert.assertTrue(mobTableDirExist(tableName));
    Assert.assertTrue(tableDirExist(tableName));

    table.close();
    TEST_UTIL.deleteTable(tableName, false);


    Assert.assertFalse(TEST_UTIL.getAdmin().tableExists(tableName));
    Assert.assertEquals(0, countMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(0, countRegionFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(0, countArchiveRegionFiles(tableName, hcd.getNameAsString(), regionInfo));
    Assert.assertFalse(mobTableDirExist(tableName));
    Assert.assertFalse(tableDirExist(tableName));
  }

  @Test
  public void testDeleteTableWithArchive() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = createTableDescriptor(tableName);
    HColumnDescriptor hcd_mob = htd.getFamily(FAMILY1);
    HColumnDescriptor hcd = htd.getFamily(FAMILY2);

    Table table = createTableWithFiles(htd);

    HRegionInfo regionInfo = TEST_UTIL.getHBaseAdmin().getTableRegions(tableName).get(0);


    // the mob file exists
    Assert.assertEquals(1, countMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(1, countRegionFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(0, countArchiveRegionFiles(tableName, hcd.getNameAsString(), regionInfo));
    Assert.assertTrue(mobTableDirExist(tableName));
    Assert.assertTrue(tableDirExist(tableName));

    table.close();
    TEST_UTIL.deleteTable(tableName, true);


    Assert.assertFalse(TEST_UTIL.getAdmin().tableExists(tableName));
    Assert.assertEquals(0, countMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(1, countArchiveMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(0, countRegionFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(1, countArchiveRegionFiles(tableName, hcd.getNameAsString(), regionInfo));
    Assert.assertFalse(mobTableDirExist(tableName));
    Assert.assertFalse(tableDirExist(tableName));
  }

  @Test
  public void testDeleteTableDefaultBehavior() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = createTableDescriptor(tableName);
    HColumnDescriptor hcd_mob = htd.getFamily(FAMILY1);
    HColumnDescriptor hcd = htd.getFamily(FAMILY2);

    Table table = createTableWithFiles(htd);

    HRegionInfo regionInfo = TEST_UTIL.getHBaseAdmin().getTableRegions(tableName).get(0);


    // the mob file exists
    Assert.assertEquals(1, countMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(0, countArchiveMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(1, countRegionFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(0, countArchiveRegionFiles(tableName, hcd.getNameAsString(), regionInfo));
    Assert.assertTrue(mobTableDirExist(tableName));
    Assert.assertTrue(tableDirExist(tableName));

    table.close();
    TEST_UTIL.deleteTable(tableName);


    Assert.assertFalse(TEST_UTIL.getAdmin().tableExists(tableName));
    Assert.assertEquals(0, countMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(1, countArchiveMobFiles(tableName, hcd_mob.getNameAsString()));
    Assert.assertEquals(0, countRegionFiles(tableName, hcd.getNameAsString()));
    Assert.assertEquals(1, countArchiveRegionFiles(tableName, hcd.getNameAsString(), regionInfo));
    Assert.assertFalse(mobTableDirExist(tableName));
    Assert.assertFalse(tableDirExist(tableName));
  }


  @Test
  public void testDeleteTableWithSnapshot() throws Exception{

    //no snapshot
    final TableName tn_no_snapshot = TableName.valueOf(name.getMethodName());
    //snapshot in progress
    final TableName tn_snapshot_inprogress = TableName.valueOf(name.getMethodName()
      +"_snapshot_inprogress");
    //complete snapshot
    final TableName tn_snapshot = TableName.valueOf(name.getMethodName()+"_snapshot");



    HTableDescriptor htd_no_snapshot = createTableDescriptor(tn_no_snapshot);
    HTableDescriptor htd_snapshot_ingress = createTableDescriptor(tn_snapshot_inprogress);
    HTableDescriptor htd_snapshot = createTableDescriptor(tn_snapshot);


    Table tb_no_snapshot = createTableWithFiles(htd_no_snapshot);
    Table tb_snapshot_ingress = createTableWithFiles(htd_snapshot_ingress);
    Table tb_snapshot = createTableWithFiles(htd_snapshot);


    String snapshotName = name.getMethodName()+"-snapshot";
    TEST_UTIL.getAdmin().snapshot(snapshotName, tn_snapshot);



    //if delete the table with snapshot in progress without archive, there should be exception
    Exception exception_withsnap_inprogress = Assert.assertThrows(DoNotRetryIOException.class,
      ()-> {
        TEST_UTIL.deleteTable(tn_snapshot, false);
      });
    Assert.assertTrue(exception_withsnap_inprogress.getMessage().
      contains("There is snapshot for the table and archive is needed"));


    // set a mock handler and make the table in the handler to mock the in process
    DisabledTableSnapshotHandler mockHandler = Mockito.mock(DisabledTableSnapshotHandler.class);
    TEST_UTIL.getMiniHBaseCluster().getMaster().getSnapshotManager()
      .setSnapshotHandlerForTesting(tn_snapshot_inprogress, mockHandler);

    //if delete the table with snapshot without archive, there should be exception
    Exception exception_withsnap = Assert.assertThrows(DoNotRetryIOException.class, ()->{
      TEST_UTIL.deleteTable(tn_snapshot_inprogress, false);
    });
    Assert.assertTrue(exception_withsnap.getMessage().
      contains("There is snapshot for the table and archive is needed"));


    //test with correct step
    TEST_UTIL.deleteTable(tn_no_snapshot, false);
    TEST_UTIL.deleteTable(tn_snapshot_inprogress, true);
    TEST_UTIL.deleteTable(tn_snapshot, true);

    tb_no_snapshot.close();
    tb_snapshot_ingress.close();
    tb_snapshot.close();


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


  private int countRegionFiles(TableName tn, String familyName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();

    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());

    if(TEST_UTIL.getHBaseAdmin().getTableRegions(tn).isEmpty()) {
      return 0;
    }

    HRegionInfo regionInfo = TEST_UTIL.getHBaseAdmin().getTableRegions(tn).get(0);

    Path regionDir = FSUtils.getRegionDirFromRootDir(rootDir, regionInfo);
    Path nfDir = new Path(regionDir,familyName);

    if (fs.exists(nfDir)) {
      return fs.listStatus(nfDir).length;
    }
    return 0;
  }

  private int countArchiveRegionFiles(TableName tn, String familyName, HRegionInfo regionInfo)
    throws IOException {

    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path storePath = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(), tn,
      regionInfo.getEncodedName(), familyName);

    if (fs.exists(storePath)) {
      return fs.listStatus(storePath).length;
    }
    return 0;
  }

  private boolean tableDirExist(TableName tn) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();

    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tn);
    return fs.exists(tableDir);
  }
}

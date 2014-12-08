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

package org.apache.hadoop.hbase.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.migration.NamespaceUpgrade;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test migration that changes HRI serialization into PB. Tests by bringing up a cluster from actual
 * data from a 0.92 cluster, as well as manually downgrading and then upgrading the hbase:meta info.
 * @deprecated Remove after 0.96
 */
@Category(MediumTests.class)
@Deprecated
public class TestMetaMigrationConvertingToPB {
  static final Log LOG = LogFactory.getLog(TestMetaMigrationConvertingToPB.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static String TESTTABLE = "TestTable";

  private final static int ROW_COUNT = 100;
  private final static int REGION_COUNT = 9; //initial number of regions of the TestTable

  private static final int META_VERSION_092 = 0;

  /*
   * This test uses a tgz file named "TestMetaMigrationConvertingToPB.tgz" under
   * hbase-server/src/test/data which contains file data from a 0.92 cluster.
   * The cluster has a table named "TestTable", which has 100 rows. 0.94 has same
   * hbase:meta structure, so it should be the same.
   *
   * hbase(main):001:0> create 'TestTable', 'f1'
   * hbase(main):002:0> for i in 1..100
   * hbase(main):003:1> put 'TestTable', "row#{i}", "f1:c1", i
   * hbase(main):004:1> end
   *
   * There are 9 regions in the table
   */

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start up our mini cluster on top of an 0.92 root.dir that has data from
    // a 0.92 hbase run -- it has a table with 100 rows in it  -- and see if
    // we can migrate from 0.92
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    Path testdir = TEST_UTIL.getDataTestDir("TestMetaMigrationConvertToPB");
    // Untar our test dir.
    File untar = untar(new File(testdir.toString()));
    // Now copy the untar up into hdfs so when we start hbase, we'll run from it.
    Configuration conf = TEST_UTIL.getConfiguration();
    FsShell shell = new FsShell(conf);
    FileSystem fs = FileSystem.get(conf);
    // find where hbase will root itself, so we can copy filesystem there
    Path hbaseRootDir = TEST_UTIL.getDefaultRootDirPath();
    if (!fs.isDirectory(hbaseRootDir.getParent())) {
      // mkdir at first
      fs.mkdirs(hbaseRootDir.getParent());
    }
    doFsCommand(shell,
      new String [] {"-put", untar.toURI().toString(), hbaseRootDir.toString()});

    //windows fix: tgz file has hbase:meta directory renamed as -META- since the original is an illegal
    //name under windows. So we rename it back. See src/test/data//TestMetaMigrationConvertingToPB.README and
    //https://issues.apache.org/jira/browse/HBASE-6821
    doFsCommand(shell, new String [] {"-mv", new Path(hbaseRootDir, "-META-").toString(),
      new Path(hbaseRootDir, ".META.").toString()});
    // See whats in minihdfs.
    doFsCommand(shell, new String [] {"-lsr", "/"});

    //upgrade to namespace as well
    Configuration toolConf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_DIR, TEST_UTIL.getDefaultRootDirPath().toString());
    ToolRunner.run(toolConf, new NamespaceUpgrade(), new String[]{"--upgrade"});

    TEST_UTIL.startMiniHBaseCluster(1, 1);
    // Assert we are running against the copied-up filesystem.  The copied-up
    // rootdir should have had a table named 'TestTable' in it.  Assert it
    // present.
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TESTTABLE);
    ResultScanner scanner = t.getScanner(new Scan());
    int count = 0;
    while (scanner.next() != null) {
      count++;
    }
    // Assert that we find all 100 rows that are in the data we loaded.  If
    // so then we must have migrated it from 0.90 to 0.92.
    Assert.assertEquals(ROW_COUNT, count);
    scanner.close();
    t.close();
  }

  private static File untar(final File testdir) throws IOException {
    // Find the src data under src/test/data
    final String datafile = "TestMetaMigrationConvertToPB";
    String srcTarFile =
      System.getProperty("project.build.testSourceDirectory", "src/test") +
      File.separator + "data" + File.separator + datafile + ".tgz";
    File homedir = new File(testdir.toString());
    File tgtUntarDir = new File(homedir, datafile);
    if (tgtUntarDir.exists()) {
      if (!FileUtil.fullyDelete(tgtUntarDir)) {
        throw new IOException("Failed delete of " + tgtUntarDir.toString());
      }
    }
    LOG.info("Untarring " + srcTarFile + " into " + homedir.toString());
    FileUtil.unTar(new File(srcTarFile), homedir);
    Assert.assertTrue(tgtUntarDir.exists());
    return tgtUntarDir;
  }

  private static void doFsCommand(final FsShell shell, final String [] args)
  throws Exception {
    // Run the 'put' command.
    int errcode = shell.run(args);
    if (errcode != 0) throw new IOException("Failed put; errcode=" + errcode);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaUpdatedFlagInROOT() throws Exception {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    boolean metaUpdated = MetaMigrationConvertingToPB.
      isMetaTableUpdated(master.getCatalogTracker());
    assertEquals(true, metaUpdated);
    verifyMetaRowsAreUpdated(master.getCatalogTracker());
  }

  @Test
  public void testMetaMigration() throws Exception {
    LOG.info("Starting testMetaMigration");
    final byte [] FAMILY = Bytes.toBytes("family");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testMetaMigration"));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      htd.addFamily(hcd);
    Configuration conf = TEST_UTIL.getConfiguration();
    byte[][] regionNames = new byte[][]{
        HConstants.EMPTY_START_ROW,
        Bytes.toBytes("region_a"),
        Bytes.toBytes("region_b")};
    createMultiRegionsWithWritableSerialization(conf,
        htd.getTableName().getName(),
        regionNames);
    CatalogTracker ct =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogTracker();
    // Erase the current version of root meta for this test.
    undoVersionInRoot(ct);
    MetaReader.fullScanMetaAndPrint(ct);
    LOG.info("Meta Print completed.testMetaMigration");

    long numMigratedRows = MetaMigrationConvertingToPB.updateMeta(
        TEST_UTIL.getHBaseCluster().getMaster());
    MetaReader.fullScanMetaAndPrint(ct);

    // Should be one entry only and it should be for the table we just added.
    assertEquals(regionNames.length, numMigratedRows);

    // Assert that the flag in ROOT is updated to reflect the correct status
    boolean metaUpdated =
        MetaMigrationConvertingToPB.isMetaTableUpdated(
        TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogTracker());
    assertEquals(true, metaUpdated);
    verifyMetaRowsAreUpdated(ct);
  }

  /**
   * This test assumes a master crash/failure during the meta migration process
   * and attempts to continue the meta migration process when a new master takes over.
   * When a master dies during the meta migration we will have some rows of
   * META.CatalogFamily updated with PB serialization and some
   * still hanging with writable serialization. When the backup master/ or
   * fresh start of master attempts the migration it will encounter some rows of META
   * already updated with new HRI and some still legacy. This test will simulate this
   * scenario and validates that the migration process can safely skip the updated
   * rows and migrate any pending rows at startup.
   * @throws Exception
   */
  @Test
  public void testMasterCrashDuringMetaMigration() throws Exception {
    final byte[] FAMILY = Bytes.toBytes("family");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf
        ("testMasterCrashDuringMetaMigration"));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      htd.addFamily(hcd);
    Configuration conf = TEST_UTIL.getConfiguration();
    // Create 10 New regions.
    createMultiRegionsWithPBSerialization(conf, htd.getTableName().getName(), 10);
    // Create 10 Legacy regions.
    createMultiRegionsWithWritableSerialization(conf,
        htd.getTableName().getName(), 10);
    CatalogTracker ct =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogTracker();
    // Erase the current version of root meta for this test.
    undoVersionInRoot(ct);

    MetaReader.fullScanMetaAndPrint(ct);
    LOG.info("Meta Print completed.testUpdatesOnMetaWithLegacyHRI");

    long numMigratedRows =
        MetaMigrationConvertingToPB.updateMetaIfNecessary(
            TEST_UTIL.getHBaseCluster().getMaster());
    assertEquals(numMigratedRows, 10);

    // Assert that the flag in ROOT is updated to reflect the correct status
    boolean metaUpdated = MetaMigrationConvertingToPB.
      isMetaTableUpdated(TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogTracker());
    assertEquals(true, metaUpdated);

    verifyMetaRowsAreUpdated(ct);

    LOG.info("END testMasterCrashDuringMetaMigration");
  }

  /**
   * Verify that every hbase:meta row is updated
   */
  void verifyMetaRowsAreUpdated(CatalogTracker catalogTracker)
      throws IOException {
    List<Result> results = MetaReader.fullScan(catalogTracker);
    assertTrue(results.size() >= REGION_COUNT);

    for (Result result : results) {
      byte[] hriBytes = result.getValue(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
      assertTrue(hriBytes != null && hriBytes.length > 0);
      assertTrue(MetaMigrationConvertingToPB.isMigrated(hriBytes));

      byte[] splitA = result.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SPLITA_QUALIFIER);
      if (splitA != null && splitA.length > 0) {
        assertTrue(MetaMigrationConvertingToPB.isMigrated(splitA));
      }

      byte[] splitB = result.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SPLITB_QUALIFIER);
      if (splitB != null && splitB.length > 0) {
        assertTrue(MetaMigrationConvertingToPB.isMigrated(splitB));
      }
    }
  }

  /** Changes the version of hbase:meta to 0 to simulate 0.92 and 0.94 clusters*/
  private void undoVersionInRoot(CatalogTracker ct) throws IOException {
    Put p = new Put(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());

    p.add(HConstants.CATALOG_FAMILY, HConstants.META_VERSION_QUALIFIER,
        Bytes.toBytes(META_VERSION_092));

    // TODO wire this MetaEditor.putToRootTable(ct, p);
    LOG.info("Downgraded -ROOT- meta version=" + META_VERSION_092);
  }

  /**
   * Inserts multiple regions into hbase:meta using Writable serialization instead of PB
   */
  public int createMultiRegionsWithWritableSerialization(final Configuration c,
      final byte[] tableName, int numRegions) throws IOException {
    if (numRegions < 3) throw new IOException("Must create at least 3 regions");
    byte [] startKey = Bytes.toBytes("aaaaa");
    byte [] endKey = Bytes.toBytes("zzzzz");
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    byte [][] regionStartKeys = new byte[splitKeys.length+1][];
    for (int i=0;i<splitKeys.length;i++) {
      regionStartKeys[i+1] = splitKeys[i];
    }
    regionStartKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    return createMultiRegionsWithWritableSerialization(c, tableName, regionStartKeys);
  }

  public int createMultiRegionsWithWritableSerialization(final Configuration c,
      final byte[] tableName, byte [][] startKeys)
  throws IOException {
    return createMultiRegionsWithWritableSerialization(c,
        TableName.valueOf(tableName), startKeys);
  }

  /**
   * Inserts multiple regions into hbase:meta using Writable serialization instead of PB
   */
  public int createMultiRegionsWithWritableSerialization(final Configuration c,
      final TableName tableName, byte [][] startKeys)
  throws IOException {
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    HTable meta = new HTable(c, TableName.META_TABLE_NAME);

    List<HRegionInfo> newRegions
        = new ArrayList<HRegionInfo>(startKeys.length);
    int count = 0;
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      HRegionInfo hri = new HRegionInfo(tableName, startKeys[i], startKeys[j]);
      Put put = new Put(hri.getRegionName());
      put.setDurability(Durability.SKIP_WAL);
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        getBytes(hri)); //this is the old Writable serialization

      //also add the region as it's daughters
      put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER,
          getBytes(hri)); //this is the old Writable serialization

      put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER,
          getBytes(hri)); //this is the old Writable serialization

      meta.put(put);
      LOG.info("createMultiRegionsWithWritableSerialization: PUT inserted " + hri.toString());

      newRegions.add(hri);
      count++;
    }
    meta.close();
    return count;
  }

  @Deprecated
  private byte[] getBytes(HRegionInfo hri) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    try {
      hri.write(out);
      return out.getData();
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Inserts multiple regions into hbase:meta using PB serialization
   */
  int createMultiRegionsWithPBSerialization(final Configuration c,
      final byte[] tableName, int numRegions)
  throws IOException {
    if (numRegions < 3) throw new IOException("Must create at least 3 regions");
    byte [] startKey = Bytes.toBytes("aaaaa");
    byte [] endKey = Bytes.toBytes("zzzzz");
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    byte [][] regionStartKeys = new byte[splitKeys.length+1][];
    for (int i=0;i<splitKeys.length;i++) {
      regionStartKeys[i+1] = splitKeys[i];
    }
    regionStartKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    return createMultiRegionsWithPBSerialization(c, tableName, regionStartKeys);
  }

  /**
   * Inserts multiple regions into hbase:meta using PB serialization
   */
  int createMultiRegionsWithPBSerialization(final Configuration c, final byte[] tableName,
      byte [][] startKeys) throws IOException {
    return createMultiRegionsWithPBSerialization(c,
        TableName.valueOf(tableName), startKeys);
  }

  int createMultiRegionsWithPBSerialization(final Configuration c,
      final TableName tableName,
      byte [][] startKeys) throws IOException {
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    HTable meta = new HTable(c, TableName.META_TABLE_NAME);

    List<HRegionInfo> newRegions
        = new ArrayList<HRegionInfo>(startKeys.length);
    int count = 0;
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      HRegionInfo hri = new HRegionInfo(tableName, startKeys[i], startKeys[j]);
      Put put = MetaEditor.makePutFromRegionInfo(hri);
      put.setDurability(Durability.SKIP_WAL);
      meta.put(put);
      LOG.info("createMultiRegionsWithPBSerialization: PUT inserted " + hri.toString());

      newRegions.add(hri);
      count++;
    }
    meta.close();
    return count;
  }


}

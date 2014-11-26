/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test upgrade from no namespace in 0.94 to namespace directory structure.
 * Mainly tests that tables are migrated and consistent. Also verifies
 * that snapshots have been migrated correctly.
 *
 * <p>Uses a tarball which is an image of an 0.94 hbase.rootdir.
 *
 * <p>Contains tables with currentKeys as the stored keys:
 * foo, ns1.foo, ns2.foo
 *
 * <p>Contains snapshots with snapshot{num}Keys as the contents:
 * snapshot1Keys, snapshot2Keys
 *
 * Image also contains _acl_ table with one region and two storefiles.
 * This is needed to test the acl table migration.
 *
 */
@Category(MediumTests.class)
public class TestNamespaceUpgrade {
  static final Log LOG = LogFactory.getLog(TestNamespaceUpgrade.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String snapshot1Keys[] =
      {"1","10","2","3","4","5","6","7","8","9"};
  private final static String snapshot2Keys[] =
      {"1","2","3","4","5","6","7","8","9"};
  private final static String currentKeys[] =
      {"1","2","3","4","5","6","7","8","9","A"};
  private final static TableName tables[] =
    {TableName.valueOf("data"), TableName.valueOf("foo"),
     TableName.valueOf("ns1.foo"), TableName.valueOf("ns.two.foo")};

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start up our mini cluster on top of an 0.94 root.dir that has data from
    // a 0.94 hbase run and see if we can migrate to 0.96
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    Path testdir = TEST_UTIL.getDataTestDir("TestNamespaceUpgrade");
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
    if(org.apache.hadoop.util.VersionInfo.getVersion().startsWith("2.")) {
      LOG.info("Hadoop version is 2.x, pre-migrating snapshot dir");
      FileSystem localFS = FileSystem.getLocal(conf);
      if(!localFS.rename(new Path(untar.toString(), HConstants.OLD_SNAPSHOT_DIR_NAME),
          new Path(untar.toString(), HConstants.SNAPSHOT_DIR_NAME))) {
        throw new IllegalStateException("Failed to move snapshot dir to 2.x expectation");
      }
    }
    doFsCommand(shell,
      new String [] {"-put", untar.toURI().toString(), hbaseRootDir.toString()});
    doFsCommand(shell, new String [] {"-lsr", "/"});
    // See whats in minihdfs.
    Configuration toolConf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_DIR, TEST_UTIL.getDefaultRootDirPath().toString());
    ToolRunner.run(toolConf, new NamespaceUpgrade(), new String[]{"--upgrade"});
    assertTrue(FSUtils.getVersion(fs, hbaseRootDir).equals(HConstants.FILE_SYSTEM_VERSION));
    doFsCommand(shell, new String [] {"-lsr", "/"});
    TEST_UTIL.startMiniHBaseCluster(1, 1);

    for(TableName table: tables) {
      int count = 0;
      for(Result res: new HTable(TEST_UTIL.getConfiguration(), table).getScanner(new Scan())) {
        assertEquals(currentKeys[count++], Bytes.toString(res.getRow()));
      }
      Assert.assertEquals(currentKeys.length, count);
    }
    assertEquals(2, TEST_UTIL.getHBaseAdmin().listNamespaceDescriptors().length);

    //verify ACL table is migrated
    HTable secureTable = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    ResultScanner scanner = secureTable.getScanner(new Scan());
    int count = 0;
    for(Result r : scanner) {
      count++;
    }
    assertEquals(3, count);
    assertFalse(TEST_UTIL.getHBaseAdmin().tableExists(TableName.valueOf("_acl_")));

    //verify ACL table was compacted
    List<HRegion> regions = TEST_UTIL.getMiniHBaseCluster().getRegions(secureTable.getName());
    for(HRegion region : regions) {
      assertEquals(1, region.getStores().size());
    }
  }

   static File untar(final File testdir) throws IOException {
    // Find the src data under src/test/data
    final String datafile = "TestNamespaceUpgrade";
    File srcTarFile = new File(
      System.getProperty("project.build.testSourceDirectory", "src/test") +
      File.separator + "data" + File.separator + datafile + ".tgz");
    File homedir = new File(testdir.toString());
    File tgtUntarDir = new File(homedir, "hbase");
    if (tgtUntarDir.exists()) {
      if (!FileUtil.fullyDelete(tgtUntarDir)) {
        throw new IOException("Failed delete of " + tgtUntarDir.toString());
      }
    }
    if (!srcTarFile.exists()) {
      throw new IOException(srcTarFile+" does not exist");
    }
    LOG.info("Untarring " + srcTarFile + " into " + homedir.toString());
    FileUtil.unTar(srcTarFile, homedir);
    Assert.assertTrue(tgtUntarDir.exists());
    return tgtUntarDir;
  }

  private static void doFsCommand(final FsShell shell, final String [] args)
  throws Exception {
    // Run the 'put' command.
    int errcode = shell.run(args);
    if (errcode != 0) throw new IOException("Failed put; errcode=" + errcode);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout=300000)
  public void testSnapshots() throws IOException, InterruptedException {
    String snapshots[][] = {snapshot1Keys, snapshot2Keys};
    for(int i = 1; i <= snapshots.length; i++) {
      for(TableName table: tables) {
        TEST_UTIL.getHBaseAdmin().cloneSnapshot(table+"_snapshot"+i, TableName.valueOf(table+"_clone"+i));
        FSUtils.logFileSystemState(FileSystem.get(TEST_UTIL.getConfiguration()),
            FSUtils.getRootDir(TEST_UTIL.getConfiguration()),
            LOG);
        int count = 0;
        for(Result res: new HTable(TEST_UTIL.getConfiguration(), table+"_clone"+i).getScanner(new
            Scan())) {
          assertEquals(snapshots[i-1][count++], Bytes.toString(res.getRow()));
        }
        Assert.assertEquals(table+"_snapshot"+i, snapshots[i-1].length, count);
      }
    }
  }

  @Test (timeout=300000)
  public void testRenameUsingSnapshots() throws Exception {
    String newNS = "newNS";
    TEST_UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(newNS).build());
    for(TableName table: tables) {
      int count = 0;
      for(Result res: new HTable(TEST_UTIL.getConfiguration(), table).getScanner(new
          Scan())) {
        assertEquals(currentKeys[count++], Bytes.toString(res.getRow()));
      }
      TEST_UTIL.getHBaseAdmin().snapshot(table + "_snapshot3", table);
      final TableName newTableName =
        TableName.valueOf(newNS + TableName.NAMESPACE_DELIM + table + "_clone3");
      TEST_UTIL.getHBaseAdmin().cloneSnapshot(table + "_snapshot3", newTableName);
      Thread.sleep(1000);
      count = 0;
      for(Result res: new HTable(TEST_UTIL.getConfiguration(), newTableName).getScanner(new
          Scan())) {
        assertEquals(currentKeys[count++], Bytes.toString(res.getRow()));
      }
      FSUtils.logFileSystemState(TEST_UTIL.getTestFileSystem(), TEST_UTIL.getDefaultRootDirPath()
          , LOG);
      Assert.assertEquals(newTableName + "", currentKeys.length, count);
      TEST_UTIL.getHBaseAdmin().flush(newTableName);
      TEST_UTIL.getHBaseAdmin().majorCompact(newTableName);
      TEST_UTIL.waitFor(30000, new Waiter.Predicate<IOException>() {
        @Override
        public boolean evaluate() throws IOException {
          return TEST_UTIL.getHBaseAdmin().getCompactionState(newTableName) ==
              AdminProtos.GetRegionInfoResponse.CompactionState.NONE;
        }
      });
    }

    String nextNS = "nextNS";
    TEST_UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(nextNS).build());
    for(TableName table: tables) {
      TableName srcTable = TableName.valueOf(newNS + TableName.NAMESPACE_DELIM + table + "_clone3");
      TEST_UTIL.getHBaseAdmin().snapshot(table + "_snapshot4", srcTable);
      TableName newTableName =
        TableName.valueOf(nextNS + TableName.NAMESPACE_DELIM + table + "_clone4");
      TEST_UTIL.getHBaseAdmin().cloneSnapshot(table+"_snapshot4", newTableName);
      FSUtils.logFileSystemState(TEST_UTIL.getTestFileSystem(), TEST_UTIL.getDefaultRootDirPath(),
        LOG);
      int count = 0;
      for(Result res: new HTable(TEST_UTIL.getConfiguration(), newTableName).getScanner(new
          Scan())) {
        assertEquals(currentKeys[count++], Bytes.toString(res.getRow()));
      }
      Assert.assertEquals(newTableName + "", currentKeys.length, count);
    }
  }

  @Test (timeout=300000)
  public void testOldDirsAreGonePostMigration() throws IOException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Path hbaseRootDir = TEST_UTIL.getDefaultRootDirPath();
    List <String> dirs = new ArrayList<String>(NamespaceUpgrade.NON_USER_TABLE_DIRS);
    // Remove those that are not renamed
    dirs.remove(HConstants.HBCK_SIDELINEDIR_NAME);
    dirs.remove(HConstants.SNAPSHOT_DIR_NAME);
    dirs.remove(HConstants.HBASE_TEMP_DIRECTORY);
    for (String dir: dirs) {
      assertFalse(fs.exists(new Path(hbaseRootDir, dir)));
    }
  }

  @Test (timeout=300000)
  public void testNewDirsArePresentPostMigration() throws IOException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    // Below list does not include 'corrupt' because there is no 'corrupt' in the tgz
    String [] newdirs = new String [] {HConstants.BASE_NAMESPACE_DIR,
      HConstants.HREGION_LOGDIR_NAME};
    Path hbaseRootDir = TEST_UTIL.getDefaultRootDirPath();
    for (String dir: newdirs) {
      assertTrue(dir, fs.exists(new Path(hbaseRootDir, dir)));
    }
  }

  @Test (timeout = 300000)
  public void testACLTableMigration() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testACLTable");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    byte[] FAMILY = Bytes.toBytes("l");
    byte[] QUALIFIER = Bytes.toBytes("testUser");
    byte[] VALUE = Bytes.toBytes("RWCA");

    // Create a Region
    HTableDescriptor aclTable = new HTableDescriptor(TableName.valueOf("testACLTable"));
    aclTable.addFamily(new HColumnDescriptor(FAMILY));
    FSTableDescriptors fstd = new FSTableDescriptors(fs, rootDir);
    fstd.createTableDescriptor(aclTable);
    HRegionInfo hriAcl = new HRegionInfo(aclTable.getTableName(), null, null);
    HRegion region = HRegion.createHRegion(hriAcl, rootDir, conf, aclTable);
    try {
      // Create rows
      Put p = new Put(Bytes.toBytes("-ROOT-"));
      p.addImmutable(FAMILY, QUALIFIER, VALUE);
      region.put(p);
      p = new Put(Bytes.toBytes(".META."));
      p.addImmutable(FAMILY, QUALIFIER, VALUE);
      region.put(p);
      p = new Put(Bytes.toBytes("_acl_"));
      p.addImmutable(FAMILY, QUALIFIER, VALUE);
      region.put(p);

      NamespaceUpgrade upgrade = new NamespaceUpgrade();
      upgrade.updateAcls(region);

      // verify rows -ROOT- is removed
      Get g = new Get(Bytes.toBytes("-ROOT-"));
      Result r = region.get(g);
      assertTrue(r == null || r.size() == 0);

      // verify rows _acl_ is renamed to hbase:acl
      g = new Get(AccessControlLists.ACL_TABLE_NAME.toBytes());
      r = region.get(g);
      assertTrue(r != null && r.size() == 1);
      assertTrue(Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)) == 0);

      // verify rows .META. is renamed to hbase:meta
      g = new Get(TableName.META_TABLE_NAME.toBytes());
      r = region.get(g);
      assertTrue(r != null && r.size() == 1);
      assertTrue(Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)) == 0);
    } finally {
      region.close();
      // Delete the region
      HRegionFileSystem.deleteRegionFromFileSystem(conf, fs,
        FSUtils.getTableDir(rootDir, hriAcl.getTable()), hriAcl);
    }
  }
}

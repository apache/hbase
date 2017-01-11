/*
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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionFileSystem {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestHRegionFileSystem.class);
  private static final byte[][] FAMILIES = {
    Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-A")),
    Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-B")) };
  private static final TableName TABLE_NAME = TableName.valueOf("TestTable");

  @Test
  public void testBlockStoragePolicy() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster();
    HTable table = (HTable) TEST_UTIL.createTable(TABLE_NAME, FAMILIES);
    assertEquals("Should start with empty table", 0, TEST_UTIL.countRows(table));
    HRegionFileSystem regionFs = getHRegionFS(table, conf);
    // the original block storage policy would be HOT
    String spA = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[0]));
    String spB = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[1]));
    LOG.debug("Storage policy of cf 0: [" + spA + "].");
    LOG.debug("Storage policy of cf 1: [" + spB + "].");
    assertEquals("HOT", spA);
    assertEquals("HOT", spB);

    // Recreate table and make sure storage policy could be set through configuration
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getConfiguration().set(HStore.BLOCK_STORAGE_POLICY_KEY, "WARM");
    TEST_UTIL.startMiniCluster();
    table = (HTable) TEST_UTIL.createTable(TABLE_NAME, FAMILIES);
    regionFs = getHRegionFS(table, conf);

    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      spA = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[0]));
      spB = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[1]));
      LOG.debug("Storage policy of cf 0: [" + spA + "].");
      LOG.debug("Storage policy of cf 1: [" + spB + "].");
      assertEquals("WARM", spA);
      assertEquals("WARM", spB);

      // alter table cf schema to change storage policies
      // and make sure it could override settings in conf
      HColumnDescriptor hcdA = new HColumnDescriptor(Bytes.toString(FAMILIES[0]));
      // alter through setting HStore#BLOCK_STORAGE_POLICY_KEY in HColumnDescriptor
      hcdA.setValue(HStore.BLOCK_STORAGE_POLICY_KEY, "ONE_SSD");
      admin.modifyColumnFamily(TABLE_NAME, hcdA);
      while (TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
          .isRegionsInTransition()) {
        Thread.sleep(200);
        LOG.debug("Waiting on table to finish schema altering");
      }
      // alter through HColumnDescriptor#setStoragePolicy
      HColumnDescriptor hcdB = new HColumnDescriptor(Bytes.toString(FAMILIES[1]));
      hcdB.setStoragePolicy("ALL_SSD");
      admin.modifyColumnFamily(TABLE_NAME, hcdB);
      while (TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
          .isRegionsInTransition()) {
        Thread.sleep(200);
        LOG.debug("Waiting on table to finish schema altering");
      }
      spA = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[0]));
      spB = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[1]));
      LOG.debug("Storage policy of cf 0: [" + spA + "].");
      LOG.debug("Storage policy of cf 1: [" + spB + "].");
      assertNotNull(spA);
      assertEquals("ONE_SSD", spA);
      assertNotNull(spB);
      assertEquals("ALL_SSD", spB);

      // flush memstore snapshot into 3 files
      for (long i = 0; i < 3; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(FAMILIES[0], Bytes.toBytes(i), Bytes.toBytes(i));
        table.put(put);
        admin.flush(TABLE_NAME);
      }
      // there should be 3 files in store dir
      FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
      Path storePath = regionFs.getStoreDir(Bytes.toString(FAMILIES[0]));
      FileStatus[] storeFiles = FSUtils.listStatus(fs, storePath);
      assertNotNull(storeFiles);
      assertEquals(3, storeFiles.length);
      // store temp dir still exists but empty
      Path storeTempDir = new Path(regionFs.getTempDir(), Bytes.toString(FAMILIES[0]));
      assertTrue(fs.exists(storeTempDir));
      FileStatus[] tempFiles = FSUtils.listStatus(fs, storeTempDir);
      assertNull(tempFiles);
      // storage policy of cf temp dir and 3 store files should be ONE_SSD
      assertEquals("ONE_SSD",
        ((HFileSystem) regionFs.getFileSystem()).getStoragePolicyName(storeTempDir));
      for (FileStatus status : storeFiles) {
        assertEquals("ONE_SSD",
          ((HFileSystem) regionFs.getFileSystem()).getStoragePolicyName(status.getPath()));
      }

      // change storage policies by calling raw api directly
      regionFs.setStoragePolicy(Bytes.toString(FAMILIES[0]), "ALL_SSD");
      regionFs.setStoragePolicy(Bytes.toString(FAMILIES[1]), "ONE_SSD");
      spA = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[0]));
      spB = regionFs.getStoragePolicyName(Bytes.toString(FAMILIES[1]));
      LOG.debug("Storage policy of cf 0: [" + spA + "].");
      LOG.debug("Storage policy of cf 1: [" + spB + "].");
      assertNotNull(spA);
      assertEquals("ALL_SSD", spA);
      assertNotNull(spB);
      assertEquals("ONE_SSD", spB);
    } finally {
      table.close();
      TEST_UTIL.deleteTable(TABLE_NAME);
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private HRegionFileSystem getHRegionFS(HTable table, Configuration conf) throws IOException {
    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    Path tableDir = FSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), table.getName());
    List<Path> regionDirs = FSUtils.getRegionDirs(fs, tableDir);
    assertEquals(1, regionDirs.size());
    List<Path> familyDirs = FSUtils.getFamilyDirs(fs, regionDirs.get(0));
    assertEquals(2, familyDirs.size());
    HRegionInfo hri = table.getRegionLocator().getAllRegionLocations().get(0).getRegionInfo();
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, new HFileSystem(fs), tableDir, hri);
    return regionFs;
  }

  @Test
  public void testOnDiskRegionCreation() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testOnDiskRegionCreation");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();

    // Create a Region
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("TestTable"));
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs,
        FSUtils.getTableDir(rootDir, hri.getTable()), hri);

    // Verify if the region is on disk
    Path regionDir = regionFs.getRegionDir();
    assertTrue("The region folder should be created", fs.exists(regionDir));

    // Verify the .regioninfo
    HRegionInfo hriVerify = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
    assertEquals(hri, hriVerify);

    // Open the region
    regionFs = HRegionFileSystem.openRegionFromFileSystem(conf, fs,
        FSUtils.getTableDir(rootDir, hri.getTable()), hri, false);
    assertEquals(regionDir, regionFs.getRegionDir());

    // Delete the region
    HRegionFileSystem.deleteRegionFromFileSystem(conf, fs,
        FSUtils.getTableDir(rootDir, hri.getTable()), hri);
    assertFalse("The region folder should be removed", fs.exists(regionDir));

    fs.delete(rootDir, true);
  }

  @Test
  public void testNonIdempotentOpsWithRetries() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testOnDiskRegionCreation");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();

    // Create a Region
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("TestTable"));
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, rootDir, hri);
    assertTrue(fs.exists(regionFs.getRegionDir()));

    regionFs = new HRegionFileSystem(conf, new MockFileSystemForCreate(),
        null, null);
    // HRegionFileSystem.createRegionOnFileSystem(conf, new MockFileSystemForCreate(), rootDir,
    // hri);
    boolean result = regionFs.createDir(new Path("/foo/bar"));
    assertTrue("Couldn't create the directory", result);


    regionFs = new HRegionFileSystem(conf, new MockFileSystem(), null, null);
    result = regionFs.rename(new Path("/foo/bar"), new Path("/foo/bar2"));
    assertTrue("Couldn't rename the directory", result);

    regionFs = new HRegionFileSystem(conf, new MockFileSystem(), null, null);
    result = regionFs.deleteDir(new Path("/foo/bar"));
    assertTrue("Couldn't delete the directory", result);
    fs.delete(rootDir, true);
  }

  static class MockFileSystemForCreate extends MockFileSystem {
    @Override
    public boolean exists(Path path) {
      return false;
    }
  }

  /**
   * a mock fs which throws exception for first 3 times, and then process the call (returns the
   * excepted result).
   */
  static class MockFileSystem extends FileSystem {
    int retryCount;
    final static int successRetryCount = 3;

    public MockFileSystem() {
      retryCount = 0;
    }

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
      throw new IOException("");
    }

    @Override
    public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3,
        short arg4, long arg5, Progressable arg6) throws IOException {
      LOG.debug("Create, " + retryCount);
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return null;
    }

    @Override
    public boolean delete(Path arg0) throws IOException {
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) throws IOException {
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public FileStatus getFileStatus(Path arg0) throws IOException {
      FileStatus fs = new FileStatus();
      return fs;
    }

    @Override
    public boolean exists(Path path) {
      return true;
    }

    @Override
    public URI getUri() {
      throw new RuntimeException("Something bad happen");
    }

    @Override
    public Path getWorkingDirectory() {
      throw new RuntimeException("Something bad happen");
    }

    @Override
    public FileStatus[] listStatus(Path arg0) throws IOException {
      throw new IOException("Something bad happen");
    }

    @Override
    public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
      LOG.debug("mkdirs, " + retryCount);
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public FSDataInputStream open(Path arg0, int arg1) throws IOException {
      throw new IOException("Something bad happen");
    }

    @Override
    public boolean rename(Path arg0, Path arg1) throws IOException {
      LOG.debug("rename, " + retryCount);
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public void setWorkingDirectory(Path arg0) {
      throw new RuntimeException("Something bad happen");
    }
  }

  @Test
  public void testTempAndCommit() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testTempAndCommit");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();

    // Create a Region
    String familyName = "cf";
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("TestTable"));
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, rootDir, hri);

    // New region, no store files
    Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(familyName);
    assertEquals(0, storeFiles != null ? storeFiles.size() : 0);

    // Create a new file in temp (no files in the family)
    Path buildPath = regionFs.createTempName();
    fs.createNewFile(buildPath);
    storeFiles = regionFs.getStoreFiles(familyName);
    assertEquals(0, storeFiles != null ? storeFiles.size() : 0);

    // commit the file
    Path dstPath = regionFs.commitStoreFile(familyName, buildPath);
    storeFiles = regionFs.getStoreFiles(familyName);
    assertEquals(0, storeFiles != null ? storeFiles.size() : 0);
    assertFalse(fs.exists(buildPath));

    fs.delete(rootDir, true);
  }
}

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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.MockServer;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test the HFileLink Cleaner. HFiles with links cannot be deleted until a link is present.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestHFileLinkCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileLinkCleaner.class);

  private Configuration conf;
  private Path rootDir;
  private FileSystem fs;
  private TableName tableName;
  private TableName tableLinkName;
  private String hfileName;
  private String familyName;
  private RegionInfo hri;
  private RegionInfo hriLink;
  private Path archiveDir;
  private Path archiveStoreDir;
  private Path familyPath;
  private Path hfilePath;
  private Path familyLinkPath;
  private String hfileLinkName;
  private Path linkBackRefDir;
  private Path linkBackRef;
  private FileStatus[] backRefs;
  private HFileCleaner cleaner;
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static DirScanPool POOL;
  private static final long TTL = 1000;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() {
    POOL = DirScanPool.getHFileCleanerScanPool(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() {
    POOL.shutdownNow();
  }

  @Before
  public void configureDirectoriesAndLinks() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    CommonFSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
    conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, HFileLinkCleaner.class.getName());
    rootDir = CommonFSUtils.getRootDir(conf);
    fs = FileSystem.get(conf);

    tableName = TableName.valueOf(name.getMethodName());
    tableLinkName = TableName.valueOf(name.getMethodName() + "-link");
    hfileName = "1234567890";
    familyName = "cf";

    hri = RegionInfoBuilder.newBuilder(tableName).build();
    hriLink = RegionInfoBuilder.newBuilder(tableLinkName).build();

    archiveDir = HFileArchiveUtil.getArchivePath(conf);
    archiveStoreDir =
      HFileArchiveUtil.getStoreArchivePath(conf, tableName, hri.getEncodedName(), familyName);

    // Create hfile /hbase/table-link/region/cf/getEncodedName.HFILE(conf);
    familyPath = getFamilyDirPath(archiveDir, tableName, hri.getEncodedName(), familyName);
    fs.mkdirs(familyPath);
    hfilePath = new Path(familyPath, hfileName);
    fs.createNewFile(hfilePath);

    HRegionFileSystem regionFS = HRegionFileSystem.create(conf, fs,
      CommonFSUtils.getTableDir(rootDir, tableLinkName), hriLink);
    StoreFileTracker sft = StoreFileTrackerFactory.create(conf, true,
      StoreContext.getBuilder()
        .withFamilyStoreDirectoryPath(new Path(regionFS.getRegionDir(), familyName))
        .withColumnFamilyDescriptor(ColumnFamilyDescriptorBuilder.of(familyName))
        .withRegionFileSystem(regionFS).build());
    createLink(sft, true);

    // Initialize cleaner
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, TTL);
    Server server = new DummyServer();
    cleaner = new HFileCleaner(1000, server, conf, fs, archiveDir, POOL);
  }

  private void createLink(StoreFileTracker sft, boolean createBackReference) throws IOException {
    // Create link to hfile
    familyLinkPath = getFamilyDirPath(rootDir, tableLinkName, hriLink.getEncodedName(), familyName);
    fs.mkdirs(familyLinkPath);
    sft.createHFileLink(hri.getTable(), hri.getEncodedName(), hfileName, createBackReference);
    hfileLinkName = hfileName;

    linkBackRefDir = HFileLink.getBackReferencesDir(archiveStoreDir, hfileName);
    assertTrue(fs.exists(linkBackRefDir));
    backRefs = fs.listStatus(linkBackRefDir);
    assertEquals(1, backRefs.length);
    linkBackRef = backRefs[0].getPath();
  }

  @After
  public void cleanup() throws IOException, InterruptedException {
    // HFile can be removed
    Thread.sleep(TTL * 2);
    cleaner.chore();
    assertFalse("HFile should be deleted", fs.exists(hfilePath));
    // Remove everything
    for (int i = 0; i < 4; ++i) {
      Thread.sleep(TTL * 2);
      cleaner.chore();
    }
    assertFalse("HFile should be deleted",
      fs.exists(CommonFSUtils.getTableDir(archiveDir, tableName)));
    assertFalse("Link should be deleted",
      fs.exists(CommonFSUtils.getTableDir(archiveDir, tableLinkName)));
  }

  @Test
  public void testHFileLinkCleaning() throws Exception {
    // Link backref cannot be removed
    cleaner.chore();
    // CommonFSUtils.
    assertTrue(fs.exists(linkBackRef));
    assertTrue(fs.exists(hfilePath));

    // Link backref can be removed
    fs.rename(CommonFSUtils.getTableDir(rootDir, tableLinkName),
      CommonFSUtils.getTableDir(archiveDir, tableLinkName));
    cleaner.chore();
    assertFalse("Link should be deleted", fs.exists(linkBackRef));
  }

  @Test
  public void testHFileLinkByRemovingReference() throws Exception {
    // Link backref cannot be removed
    cleaner.chore();
    assertTrue(fs.exists(linkBackRef));
    assertTrue(fs.exists(hfilePath));

    // simulate after removing the reference in data directory, the Link backref can be removed
    fs.delete(new Path(familyLinkPath, hfileLinkName), false);
    cleaner.chore();
    assertFalse("Link should be deleted", fs.exists(linkBackRef));
  }

  @Test
  public void testHFileLinkEmptyBackReferenceDirectory() throws Exception {
    // simulate and remove the back reference
    fs.delete(linkBackRef, false);
    assertTrue("back reference directory still exists", fs.exists(linkBackRefDir));
    cleaner.chore();
    assertFalse("back reference directory should be deleted", fs.exists(linkBackRefDir));
  }

  private static Path getFamilyDirPath(final Path rootDir, final TableName table,
    final String region, final String family) {
    return new Path(new Path(CommonFSUtils.getTableDir(rootDir, table), region), family);
  }

  static class DummyServer extends MockServer {

    @Override
    public Configuration getConfiguration() {
      return TEST_UTIL.getConfiguration();
    }

    @Override
    public ZKWatcher getZooKeeper() {
      try {
        return new ZKWatcher(getConfiguration(), "dummy server", this);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }
}

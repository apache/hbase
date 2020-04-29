/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * Validate if storefile length match
 * both snapshop manifest and filesystem.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotStoreFileSize {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotStoreFileSize.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("t1");
  private static final String SNAPSHOT_NAME = "s1";
  private static final String FAMILY_NAME = "cf";
  private static Configuration conf;
  private Admin admin;
  private FileSystem fs;

  @BeforeClass
  public static void setup() throws Exception {
    conf = UTIL.getConfiguration();
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void teardown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testIsStoreFileSizeMatchFilesystemAndManifest() throws IOException {
    admin = UTIL.getAdmin();
    fs = UTIL.getTestFileSystem();
    UTIL.createTable(TABLE_NAME, FAMILY_NAME.getBytes());
    Table table = admin.getConnection().getTable(TABLE_NAME);
    UTIL.loadRandomRows(table, FAMILY_NAME.getBytes(), 3, 1000);
    admin.snapshot(SNAPSHOT_NAME, TABLE_NAME);

    Map<String, Long> storeFileInfoFromManifest = new HashMap<String, Long>();
    Map<String, Long> storeFileInfoFromFS = new HashMap<String, Long>();
    String storeFileName = "";
    long storeFilesize = 0L;
    Path snapshotDir = SnapshotDescriptionUtils
        .getCompletedSnapshotDir(SNAPSHOT_NAME, UTIL.getDefaultRootDirPath());
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest snaphotManifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    List<SnapshotRegionManifest> regionManifest = snaphotManifest.getRegionManifests();
    for (int i = 0; i < regionManifest.size(); i++) {
      SnapshotRegionManifest.FamilyFiles family = regionManifest.get(i).getFamilyFiles(0);
      List<SnapshotRegionManifest.StoreFile> storeFiles = family.getStoreFilesList();
      for (int j = 0; j < storeFiles.size(); j++) {
        storeFileName = storeFiles.get(j).getName();
        storeFilesize = storeFiles.get(j).getFileSize();
        storeFileInfoFromManifest.put(storeFileName, storeFilesize);
      }
    }
    List<RegionInfo> regionsInfo = admin.getRegions(TABLE_NAME);
    Path path = CommonFSUtils.getTableDir(UTIL.getDefaultRootDirPath(), TABLE_NAME);
    for (RegionInfo regionInfo : regionsInfo) {
      HRegionFileSystem hRegionFileSystem =
          HRegionFileSystem.openRegionFromFileSystem(conf, fs, path, regionInfo, true);
      Collection<StoreFileInfo> storeFilesFS = hRegionFileSystem.getStoreFiles(FAMILY_NAME);
      Iterator<StoreFileInfo> sfIterator = storeFilesFS.iterator();
      while (sfIterator.hasNext()) {
        StoreFileInfo sfi = sfIterator.next();
        FileStatus[] fileStatus = CommonFSUtils.listStatus(fs, sfi.getPath());
        storeFileName = fileStatus[0].getPath().getName();
        storeFilesize = fileStatus[0].getLen();
        storeFileInfoFromFS.put(storeFileName, storeFilesize);
      }
    }
    Assert.assertEquals(storeFileInfoFromManifest, storeFileInfoFromFS);
  }
}

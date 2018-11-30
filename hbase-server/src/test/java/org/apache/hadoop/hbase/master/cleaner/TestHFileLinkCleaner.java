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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test the HFileLink Cleaner.
 * HFiles with links cannot be deleted until a link is present.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestHFileLinkCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileLinkCleaner.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @Test
  public void testHFileLinkCleaning() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    FSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
    conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, HFileLinkCleaner.class.getName());
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = FileSystem.get(conf);

    final TableName tableName = TableName.valueOf(name.getMethodName());
    final TableName tableLinkName = TableName.valueOf(name.getMethodName() + "-link");
    final String hfileName = "1234567890";
    final String familyName = "cf";

    HRegionInfo hri = new HRegionInfo(tableName);
    HRegionInfo hriLink = new HRegionInfo(tableLinkName);

    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    Path archiveStoreDir = HFileArchiveUtil.getStoreArchivePath(conf,
          tableName, hri.getEncodedName(), familyName);
    Path archiveLinkStoreDir = HFileArchiveUtil.getStoreArchivePath(conf,
          tableLinkName, hriLink.getEncodedName(), familyName);

    // Create hfile /hbase/table-link/region/cf/getEncodedName.HFILE(conf);
    Path familyPath = getFamilyDirPath(archiveDir, tableName, hri.getEncodedName(), familyName);
    fs.mkdirs(familyPath);
    Path hfilePath = new Path(familyPath, hfileName);
    fs.createNewFile(hfilePath);

    // Create link to hfile
    Path familyLinkPath = getFamilyDirPath(rootDir, tableLinkName,
                                        hriLink.getEncodedName(), familyName);
    fs.mkdirs(familyLinkPath);
    HFileLink.create(conf, fs, familyLinkPath, hri, hfileName);
    Path linkBackRefDir = HFileLink.getBackReferencesDir(archiveStoreDir, hfileName);
    assertTrue(fs.exists(linkBackRefDir));
    FileStatus[] backRefs = fs.listStatus(linkBackRefDir);
    assertEquals(1, backRefs.length);
    Path linkBackRef = backRefs[0].getPath();

    // Initialize cleaner
    final long ttl = 1000;
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, ttl);
    Server server = new DummyServer();
    CleanerChore.initChorePool(conf);
    HFileCleaner cleaner = new HFileCleaner(1000, server, conf, fs, archiveDir);

    // Link backref cannot be removed
    cleaner.chore();
    assertTrue(fs.exists(linkBackRef));
    assertTrue(fs.exists(hfilePath));

    // Link backref can be removed
    fs.rename(FSUtils.getTableDir(rootDir, tableLinkName),
        FSUtils.getTableDir(archiveDir, tableLinkName));
    cleaner.chore();
    assertFalse("Link should be deleted", fs.exists(linkBackRef));

    // HFile can be removed
    Thread.sleep(ttl * 2);
    cleaner.chore();
    assertFalse("HFile should be deleted", fs.exists(hfilePath));

    // Remove everything
    for (int i = 0; i < 4; ++i) {
      Thread.sleep(ttl * 2);
      cleaner.chore();
    }
    assertFalse("HFile should be deleted", fs.exists(FSUtils.getTableDir(archiveDir, tableName)));
    assertFalse("Link should be deleted", fs.exists(FSUtils.getTableDir(archiveDir, tableLinkName)));
  }

  private static Path getFamilyDirPath (final Path rootDir, final TableName table,
    final String region, final String family) {
    return new Path(new Path(FSUtils.getTableDir(rootDir, table), region), family);
  }

  static class DummyServer implements Server {

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

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf("regionserver,60020,000000");
    }

    @Override
    public void abort(String why, Throwable e) {}

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {}

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public ClusterConnection getClusterConnection() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      return null;
    }

    @Override
    public boolean isStopping() {
      return false;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return null;
    }

    @Override
    public AsyncClusterConnection getAsyncClusterConnection() {
      return null;
    }
  }
}

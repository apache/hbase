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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestHFileCleaner {
  private static final Log LOG = LogFactory.getLog(TestHFileCleaner.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    // have to use a minidfs cluster because the localfs doesn't modify file times correctly
    UTIL.startMiniDFSCluster(1);
  }

  @AfterClass
  public static void shutdownCluster() throws IOException {
    UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testTTLCleaner() throws IOException, InterruptedException {
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    Path root = UTIL.getDataTestDirOnTestFS();
    Path file = new Path(root, "file");
    fs.createNewFile(file);
    long createTime = System.currentTimeMillis();
    assertTrue("Test file not created!", fs.exists(file));
    TimeToLiveHFileCleaner cleaner = new TimeToLiveHFileCleaner();
    // update the time info for the file, so the cleaner removes it
    fs.setTimes(file, createTime - 100, -1);
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 100);
    cleaner.setConf(conf);
    assertTrue("File not set deletable - check mod time:" + getFileStats(file, fs)
        + " with create time:" + createTime, cleaner.isFileDeletable(fs.getFileStatus(file)));
  }

  /**
   * @param file to check
   * @return loggable information about the file
   */
  private String getFileStats(Path file, FileSystem fs) throws IOException {
    FileStatus status = fs.getFileStatus(file);
    return "File" + file + ", mtime:" + status.getModificationTime() + ", atime:"
        + status.getAccessTime();
  }

  @Test(timeout = 60 *1000)
  public void testHFileCleaning() throws Exception {
    final EnvironmentEdge originalEdge = EnvironmentEdgeManager.getDelegate();
    String prefix = "someHFileThatWouldBeAUUID";
    Configuration conf = UTIL.getConfiguration();
    // set TTL
    long ttl = 2000;
    conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      "org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner");
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, ttl);
    Server server = new DummyServer();
    Path archivedHfileDir = new Path(UTIL.getDataTestDirOnTestFS(), HConstants.HFILE_ARCHIVE_DIRECTORY);
    FileSystem fs = FileSystem.get(conf);
    HFileCleaner cleaner = new HFileCleaner(1000, server, conf, fs, archivedHfileDir);

    // Create 2 invalid files, 1 "recent" file, 1 very new file and 30 old files
    final long createTime = System.currentTimeMillis();
    fs.delete(archivedHfileDir, true);
    fs.mkdirs(archivedHfileDir);
    // Case 1: 1 invalid file, which should be deleted directly
    fs.createNewFile(new Path(archivedHfileDir, "dfd-dfd"));
    // Case 2: 1 "recent" file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    LOG.debug("Now is: " + createTime);
    for (int i = 1; i < 32; i++) {
      // Case 3: old files which would be deletable for the first log cleaner
      // (TimeToLiveHFileCleaner),
      Path fileName = new Path(archivedHfileDir, (prefix + "." + (createTime + i)));
      fs.createNewFile(fileName);
      // set the creation time past ttl to ensure that it gets removed
      fs.setTimes(fileName, createTime - ttl - 1, -1);
      LOG.debug("Creating " + getFileStats(fileName, fs));
    }

    // Case 2: 1 newer file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    Path saved = new Path(archivedHfileDir, prefix + ".00000000000");
    fs.createNewFile(saved);
    // set creation time within the ttl
    fs.setTimes(saved, createTime - ttl / 2, -1);
    LOG.debug("Creating " + getFileStats(saved, fs));
    for (FileStatus stat : fs.listStatus(archivedHfileDir)) {
      LOG.debug(stat.getPath().toString());
    }

    assertEquals(33, fs.listStatus(archivedHfileDir).length);

    // set a custom edge manager to handle time checking
    EnvironmentEdge setTime = new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return createTime;
      }
    };
    EnvironmentEdgeManager.injectEdge(setTime);

    // run the chore
    cleaner.chore();

    // ensure we only end up with the saved file
    assertEquals(1, fs.listStatus(archivedHfileDir).length);

    for (FileStatus file : fs.listStatus(archivedHfileDir)) {
      LOG.debug("Kept hfiles: " + file.getPath().getName());
    }

    // reset the edge back to the original edge
    EnvironmentEdgeManager.injectEdge(originalEdge);
  }

  @Test
  public void testRemovesEmptyDirectories() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // no cleaner policies = delete all files
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, "");
    Server server = new DummyServer();
    Path archivedHfileDir = new Path(UTIL.getDataTestDirOnTestFS(), HConstants.HFILE_ARCHIVE_DIRECTORY);

    // setup the cleaner
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    HFileCleaner cleaner = new HFileCleaner(1000, server, conf, fs, archivedHfileDir);

    // make all the directories for archiving files
    Path table = new Path(archivedHfileDir, "table");
    Path region = new Path(table, "regionsomthing");
    Path family = new Path(region, "fam");
    Path file = new Path(family, "file12345");
    fs.mkdirs(family);
    if (!fs.exists(family)) throw new RuntimeException("Couldn't create test family:" + family);
    fs.create(file).close();
    if (!fs.exists(file)) throw new RuntimeException("Test file didn't get created:" + file);

    // run the chore to cleanup the files (and the directories above it)
    cleaner.chore();

    // make sure all the parent directories get removed
    assertFalse("family directory not removed for empty directory", fs.exists(family));
    assertFalse("region directory not removed for empty directory", fs.exists(region));
    assertFalse("table directory not removed for empty directory", fs.exists(table));
    assertTrue("archive directory", fs.exists(archivedHfileDir));
  }

  static class DummyServer implements Server {

    @Override
    public Configuration getConfiguration() {
      return UTIL.getConfiguration();
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      try {
        return new ZooKeeperWatcher(getConfiguration(), "dummy server", this);
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
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf("regionserver,60020,000000");
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }
  }
}

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
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestHFileCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileCleaner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileCleaner.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    // have to use a minidfs cluster because the localfs doesn't modify file times correctly
    UTIL.startMiniDFSCluster(1);
    CleanerChore.initChorePool(UTIL.getConfiguration());
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

  @Test
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

  @Test
  public void testThreadCleanup() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, "");
    Server server = new DummyServer();
    Path archivedHfileDir =
        new Path(UTIL.getDataTestDirOnTestFS(), HConstants.HFILE_ARCHIVE_DIRECTORY);

    // setup the cleaner
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    HFileCleaner cleaner = new HFileCleaner(1000, server, conf, fs, archivedHfileDir);
    // clean up archive directory
    fs.delete(archivedHfileDir, true);
    fs.mkdirs(archivedHfileDir);
    // create some file to delete
    fs.createNewFile(new Path(archivedHfileDir, "dfd-dfd"));
    // launch the chore
    cleaner.chore();
    // call cleanup
    cleaner.cleanup();
    // wait awhile for thread to die
    Thread.sleep(100);
    for (Thread thread : cleaner.getCleanerThreads()) {
      Assert.assertFalse(thread.isAlive());
    }
  }

  @Test
  public void testLargeSmallIsolation() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // no cleaner policies = delete all files
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, "");
    conf.setInt(HFileCleaner.HFILE_DELETE_THROTTLE_THRESHOLD, 512 * 1024);
    Server server = new DummyServer();
    Path archivedHfileDir =
        new Path(UTIL.getDataTestDirOnTestFS(), HConstants.HFILE_ARCHIVE_DIRECTORY);

    // setup the cleaner
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    HFileCleaner cleaner = new HFileCleaner(1000, server, conf, fs, archivedHfileDir);
    // clean up archive directory
    fs.delete(archivedHfileDir, true);
    fs.mkdirs(archivedHfileDir);
    // necessary set up
    final int LARGE_FILE_NUM = 5;
    final int SMALL_FILE_NUM = 20;
    createFilesForTesting(LARGE_FILE_NUM, SMALL_FILE_NUM, fs, archivedHfileDir);
    // call cleanup
    cleaner.chore();

    Assert.assertEquals(LARGE_FILE_NUM, cleaner.getNumOfDeletedLargeFiles());
    Assert.assertEquals(SMALL_FILE_NUM, cleaner.getNumOfDeletedSmallFiles());
  }

  @Test
  public void testOnConfigurationChange() throws Exception {
    // constants
    final int ORIGINAL_THROTTLE_POINT = 512 * 1024;
    final int ORIGINAL_QUEUE_INIT_SIZE = 512;
    final int UPDATE_THROTTLE_POINT = 1024;// small enough to change large/small check
    final int UPDATE_QUEUE_INIT_SIZE = 1024;
    final int LARGE_FILE_NUM = 5;
    final int SMALL_FILE_NUM = 20;
    final int LARGE_THREAD_NUM = 2;
    final int SMALL_THREAD_NUM = 4;
    final long THREAD_TIMEOUT_MSEC = 30 * 1000L;
    final long THREAD_CHECK_INTERVAL_MSEC = 500L;

    Configuration conf = UTIL.getConfiguration();
    // no cleaner policies = delete all files
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, "");
    conf.setInt(HFileCleaner.HFILE_DELETE_THROTTLE_THRESHOLD, ORIGINAL_THROTTLE_POINT);
    conf.setInt(HFileCleaner.LARGE_HFILE_QUEUE_INIT_SIZE, ORIGINAL_QUEUE_INIT_SIZE);
    conf.setInt(HFileCleaner.SMALL_HFILE_QUEUE_INIT_SIZE, ORIGINAL_QUEUE_INIT_SIZE);
    Server server = new DummyServer();
    Path archivedHfileDir =
        new Path(UTIL.getDataTestDirOnTestFS(), HConstants.HFILE_ARCHIVE_DIRECTORY);

    // setup the cleaner
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    final HFileCleaner cleaner = new HFileCleaner(1000, server, conf, fs, archivedHfileDir);
    Assert.assertEquals(ORIGINAL_THROTTLE_POINT, cleaner.getThrottlePoint());
    Assert.assertEquals(ORIGINAL_QUEUE_INIT_SIZE, cleaner.getLargeQueueInitSize());
    Assert.assertEquals(ORIGINAL_QUEUE_INIT_SIZE, cleaner.getSmallQueueInitSize());
    Assert.assertEquals(HFileCleaner.DEFAULT_HFILE_DELETE_THREAD_TIMEOUT_MSEC,
        cleaner.getCleanerThreadTimeoutMsec());
    Assert.assertEquals(HFileCleaner.DEFAULT_HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC,
        cleaner.getCleanerThreadCheckIntervalMsec());

    // clean up archive directory and create files for testing
    fs.delete(archivedHfileDir, true);
    fs.mkdirs(archivedHfileDir);
    createFilesForTesting(LARGE_FILE_NUM, SMALL_FILE_NUM, fs, archivedHfileDir);

    // call cleaner, run as daemon to test the interrupt-at-middle case
    Thread t = new Thread() {
      @Override
      public void run() {
        cleaner.chore();
      }
    };
    t.setDaemon(true);
    t.start();
    // wait until file clean started
    while (cleaner.getNumOfDeletedSmallFiles() == 0) {
      Thread.yield();
    }

    // trigger configuration change
    Configuration newConf = new Configuration(conf);
    newConf.setInt(HFileCleaner.HFILE_DELETE_THROTTLE_THRESHOLD, UPDATE_THROTTLE_POINT);
    newConf.setInt(HFileCleaner.LARGE_HFILE_QUEUE_INIT_SIZE, UPDATE_QUEUE_INIT_SIZE);
    newConf.setInt(HFileCleaner.SMALL_HFILE_QUEUE_INIT_SIZE, UPDATE_QUEUE_INIT_SIZE);
    newConf.setInt(HFileCleaner.LARGE_HFILE_DELETE_THREAD_NUMBER, LARGE_THREAD_NUM);
    newConf.setInt(HFileCleaner.SMALL_HFILE_DELETE_THREAD_NUMBER, SMALL_THREAD_NUM);
    newConf.setLong(HFileCleaner.HFILE_DELETE_THREAD_TIMEOUT_MSEC, THREAD_TIMEOUT_MSEC);
    newConf.setLong(HFileCleaner.HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC,
        THREAD_CHECK_INTERVAL_MSEC);

    LOG.debug("File deleted from large queue: " + cleaner.getNumOfDeletedLargeFiles()
        + "; from small queue: " + cleaner.getNumOfDeletedSmallFiles());
    cleaner.onConfigurationChange(newConf);

    // check values after change
    Assert.assertEquals(UPDATE_THROTTLE_POINT, cleaner.getThrottlePoint());
    Assert.assertEquals(UPDATE_QUEUE_INIT_SIZE, cleaner.getLargeQueueInitSize());
    Assert.assertEquals(UPDATE_QUEUE_INIT_SIZE, cleaner.getSmallQueueInitSize());
    Assert.assertEquals(LARGE_THREAD_NUM + SMALL_THREAD_NUM, cleaner.getCleanerThreads().size());
    Assert.assertEquals(THREAD_TIMEOUT_MSEC, cleaner.getCleanerThreadTimeoutMsec());
    Assert.assertEquals(THREAD_CHECK_INTERVAL_MSEC, cleaner.getCleanerThreadCheckIntervalMsec());

    // make sure no cost when onConfigurationChange called with no change
    List<Thread> oldThreads = cleaner.getCleanerThreads();
    cleaner.onConfigurationChange(newConf);
    List<Thread> newThreads = cleaner.getCleanerThreads();
    Assert.assertArrayEquals(oldThreads.toArray(), newThreads.toArray());

    // wait until clean done and check
    t.join();
    LOG.debug("File deleted from large queue: " + cleaner.getNumOfDeletedLargeFiles()
        + "; from small queue: " + cleaner.getNumOfDeletedSmallFiles());
    Assert.assertTrue("Should delete more than " + LARGE_FILE_NUM
        + " files from large queue but actually " + cleaner.getNumOfDeletedLargeFiles(),
      cleaner.getNumOfDeletedLargeFiles() > LARGE_FILE_NUM);
    Assert.assertTrue("Should delete less than " + SMALL_FILE_NUM
        + " files from small queue but actually " + cleaner.getNumOfDeletedSmallFiles(),
      cleaner.getNumOfDeletedSmallFiles() < SMALL_FILE_NUM);
  }

  private void createFilesForTesting(int largeFileNum, int smallFileNum, FileSystem fs,
      Path archivedHfileDir) throws IOException {
    final Random rand = new Random();
    final byte[] large = new byte[1024 * 1024];
    for (int i = 0; i < large.length; i++) {
      large[i] = (byte) rand.nextInt(128);
    }
    final byte[] small = new byte[1024];
    for (int i = 0; i < small.length; i++) {
      small[i] = (byte) rand.nextInt(128);
    }
    // create large and small files
    for (int i = 1; i <= largeFileNum; i++) {
      FSDataOutputStream out = fs.create(new Path(archivedHfileDir, "large-file-" + i));
      out.write(large);
      out.close();
    }
    for (int i = 1; i <= smallFileNum; i++) {
      FSDataOutputStream out = fs.create(new Path(archivedHfileDir, "small-file-" + i));
      out.write(small);
      out.close();
    }
  }
}

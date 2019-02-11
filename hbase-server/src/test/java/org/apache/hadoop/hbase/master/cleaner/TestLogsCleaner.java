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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestLogsCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogsCleaner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestLogsCleaner.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final Path OLD_WALS_DIR =
      new Path(TEST_UTIL.getDataTestDir(), HConstants.HREGION_OLDLOGDIR_NAME);

  private static final Path OLD_PROCEDURE_WALS_DIR =
      new Path(OLD_WALS_DIR, "masterProcedureWALs");

  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    CleanerChore.initChorePool(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Before
  public void beforeTest() throws IOException {
    conf = TEST_UTIL.getConfiguration();

    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();

    fs.delete(OLD_WALS_DIR, true);

    // root directory
    fs.mkdirs(OLD_WALS_DIR);
  }

  /**
   * This tests verifies LogCleaner works correctly with WALs and Procedure WALs located
   * in the same oldWALs directory.
   * Created files:
   * - 2 invalid files
   * - 5 old Procedure WALs
   * - 30 old WALs from which 3 are in replication
   * - 5 recent Procedure WALs
   * - 1 recent WAL
   * - 1 very new WAL (timestamp in future)
   * - masterProcedureWALs subdirectory
   * Files which should stay:
   * - 3 replication WALs
   * - 2 new WALs
   * - 5 latest Procedure WALs
   * - masterProcedureWALs subdirectory
   */
  @Test
  public void testLogCleaning() throws Exception {
    // set TTLs
    long ttlWAL = 2000;
    long ttlProcedureWAL = 4000;
    conf.setLong("hbase.master.logcleaner.ttl", ttlWAL);
    conf.setLong("hbase.master.procedurewalcleaner.ttl", ttlProcedureWAL);

    HMaster.decorateMasterConfiguration(conf);
    Server server = new DummyServer();
    ReplicationQueueStorage queueStorage =
        ReplicationStorageFactory.getReplicationQueueStorage(server.getZooKeeper(), conf);

    String fakeMachineName = URLEncoder.encode(
        server.getServerName().toString(), StandardCharsets.UTF_8.name());

    final FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(OLD_PROCEDURE_WALS_DIR);

    final long now = System.currentTimeMillis();

    // Case 1: 2 invalid files, which would be deleted directly
    fs.createNewFile(new Path(OLD_WALS_DIR, "a"));
    fs.createNewFile(new Path(OLD_WALS_DIR, fakeMachineName + "." + "a"));

    // Case 2: 5 Procedure WALs that are old which would be deleted
    for (int i = 1; i <= 5; i++) {
      final Path fileName =
          new Path(OLD_PROCEDURE_WALS_DIR, String.format("pv2-%020d.log", i));
      fs.createNewFile(fileName);
    }

    // Sleep for sometime to get old procedure WALs
    Thread.sleep(ttlProcedureWAL - ttlWAL);

    // Case 3: old WALs which would be deletable
    for (int i = 1; i <= 30; i++) {
      Path fileName = new Path(OLD_WALS_DIR, fakeMachineName + "." + (now - i));
      fs.createNewFile(fileName);
      // Case 4: put 3 WALs in ZK indicating that they are scheduled for replication so these
      // files would pass TimeToLiveLogCleaner but would be rejected by ReplicationLogCleaner
      if (i % (30 / 3) == 0) {
        queueStorage.addWAL(server.getServerName(), fakeMachineName, fileName.getName());
        LOG.info("Replication log file: " + fileName);
      }
    }

    // Case 5: 5 Procedure WALs that are new, will stay
    for (int i = 6; i <= 10; i++) {
      Path fileName =
          new Path(OLD_PROCEDURE_WALS_DIR, String.format("pv2-%020d.log", i));
      fs.createNewFile(fileName);
    }

    // Sleep for sometime to get newer modification time
    Thread.sleep(ttlWAL);
    fs.createNewFile(new Path(OLD_WALS_DIR, fakeMachineName + "." + now));

    // Case 6: 1 newer WAL, not even deletable for TimeToLiveLogCleaner,
    // so we are not going down the chain
    fs.createNewFile(new Path(OLD_WALS_DIR, fakeMachineName + "." + (now + ttlWAL)));

    FileStatus[] status = fs.listStatus(OLD_WALS_DIR);
    LOG.info("File status: {}", Arrays.toString(status));

    // There should be 34 files and 1 masterProcedureWALs directory
    assertEquals(35, fs.listStatus(OLD_WALS_DIR).length);
    // 10 procedure WALs
    assertEquals(10, fs.listStatus(OLD_PROCEDURE_WALS_DIR).length);

    LogCleaner cleaner = new LogCleaner(1000, server, conf, fs, OLD_WALS_DIR);
    cleaner.chore();

    // In oldWALs we end up with the current WAL, a newer WAL, the 3 old WALs which
    // are scheduled for replication and masterProcedureWALs directory
    TEST_UTIL.waitFor(1000, (Waiter.Predicate<Exception>) () -> 6 == fs
        .listStatus(OLD_WALS_DIR).length);
    // In masterProcedureWALs we end up with 5 newer Procedure WALs
    TEST_UTIL.waitFor(1000, (Waiter.Predicate<Exception>) () -> 5 == fs
        .listStatus(OLD_PROCEDURE_WALS_DIR).length);

    if (LOG.isDebugEnabled()) {
      FileStatus[] statusOldWALs = fs.listStatus(OLD_WALS_DIR);
      FileStatus[] statusProcedureWALs = fs.listStatus(OLD_PROCEDURE_WALS_DIR);
      LOG.debug("Kept log file for oldWALs: {}", Arrays.toString(statusOldWALs));
      LOG.debug("Kept log file for masterProcedureWALs: {}",
          Arrays.toString(statusProcedureWALs));
    }
  }

  @Test(timeout=10000)
  public void testZooKeeperAbortDuringGetListOfReplicators() throws Exception {
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();

    List<FileStatus> dummyFiles = Arrays.asList(
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log1")),
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log2"))
    );

    FaultyZooKeeperWatcher faultyZK =
        new FaultyZooKeeperWatcher(conf, "testZooKeeperAbort-faulty", null);
    final AtomicBoolean getListOfReplicatorsFailed = new AtomicBoolean(false);

    try {
      faultyZK.init();
      ReplicationQueueStorage queueStorage = spy(ReplicationStorageFactory
          .getReplicationQueueStorage(faultyZK, conf));
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          try {
            return invocation.callRealMethod();
          } catch (ReplicationException e) {
            LOG.debug("Caught Exception", e);
            getListOfReplicatorsFailed.set(true);
            throw e;
          }
        }
      }).when(queueStorage).getAllWALs();

      cleaner.setConf(conf, faultyZK, queueStorage);
      // should keep all files due to a ConnectionLossException getting the queues znodes
      cleaner.preClean();
      Iterable<FileStatus> toDelete = cleaner.getDeletableFiles(dummyFiles);

      assertTrue(getListOfReplicatorsFailed.get());
      assertFalse(toDelete.iterator().hasNext());
      assertFalse(cleaner.isStopped());
    } finally {
      faultyZK.close();
    }
  }

  /**
   * When zk is working both files should be returned
   * @throws Exception from ZK watcher
   */
  @Test(timeout=10000)
  public void testZooKeeperNormal() throws Exception {
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();

    List<FileStatus> dummyFiles = Arrays.asList(
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log1")),
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log2"))
    );

    ZKWatcher zkw = new ZKWatcher(conf, "testZooKeeperAbort-normal", null);
    try {
      cleaner.setConf(conf, zkw);
      cleaner.preClean();
      Iterable<FileStatus> filesToDelete = cleaner.getDeletableFiles(dummyFiles);
      Iterator<FileStatus> iter = filesToDelete.iterator();
      assertTrue(iter.hasNext());
      assertEquals(new Path("log1"), iter.next().getPath());
      assertTrue(iter.hasNext());
      assertEquals(new Path("log2"), iter.next().getPath());
      assertFalse(iter.hasNext());
    } finally {
      zkw.close();
    }
  }

  @Test
  public void testOnConfigurationChange() throws Exception {
    // Prepare environments
    Server server = new DummyServer();

    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    LogCleaner cleaner = new LogCleaner(3000, server, conf, fs, OLD_WALS_DIR);
    assertEquals(LogCleaner.DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE, cleaner.getSizeOfCleaners());
    assertEquals(LogCleaner.DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
        cleaner.getCleanerThreadTimeoutMsec());
    // Create dir and files for test
    int numOfFiles = 10;
    createFiles(fs, OLD_WALS_DIR, numOfFiles);
    FileStatus[] status = fs.listStatus(OLD_WALS_DIR);
    assertEquals(numOfFiles, status.length);
    // Start cleaner chore
    Thread thread = new Thread(() -> cleaner.chore());
    thread.setDaemon(true);
    thread.start();
    // change size of cleaners dynamically
    int sizeToChange = 4;
    long threadTimeoutToChange = 30 * 1000L;
    conf.setInt(LogCleaner.OLD_WALS_CLEANER_THREAD_SIZE, sizeToChange);
    conf.setLong(LogCleaner.OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC, threadTimeoutToChange);
    cleaner.onConfigurationChange(conf);
    assertEquals(sizeToChange, cleaner.getSizeOfCleaners());
    assertEquals(threadTimeoutToChange, cleaner.getCleanerThreadTimeoutMsec());
    // Stop chore
    thread.join();
    status = fs.listStatus(OLD_WALS_DIR);
    assertEquals(0, status.length);
  }

  private void createFiles(FileSystem fs, Path parentDir, int numOfFiles) throws IOException {
    for (int i = 0; i < numOfFiles; i++) {
      // size of each file is 1M, 2M, or 3M
      int xMega = 1 + ThreadLocalRandom.current().nextInt(1, 4);
      try (FSDataOutputStream fsdos = fs.create(new Path(parentDir, "file-" + i))) {
        byte[] M = RandomUtils.nextBytes(Math.toIntExact(FileUtils.ONE_MB * xMega));
        fsdos.write(M);
      }
    }
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
    public Connection getConnection() {
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

  static class FaultyZooKeeperWatcher extends ZKWatcher {
    private RecoverableZooKeeper zk;

    public FaultyZooKeeperWatcher(Configuration conf, String identifier, Abortable abortable)
        throws ZooKeeperConnectionException, IOException {
      super(conf, identifier, abortable);
    }

    public void init() throws Exception {
      this.zk = spy(super.getRecoverableZooKeeper());
      doThrow(new KeeperException.ConnectionLossException())
        .when(zk).getChildren("/hbase/replication/rs", null);
    }

    @Override
    public RecoverableZooKeeper getRecoverableZooKeeper() {
      return zk;
    }
  }
}

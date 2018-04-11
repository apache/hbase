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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doAnswer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category(MediumTests.class)
public class TestLogsCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    CleanerChore.initChorePool(TEST_UTIL.getConfiguration());
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testLogCleaning() throws Exception{
    Configuration conf = TEST_UTIL.getConfiguration();
    // set TTL
    long ttl = 10000;
    conf.setLong("hbase.master.logcleaner.ttl", ttl);
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    Replication.decorateMasterConfiguration(conf);
    Server server = new DummyServer();
    ReplicationQueues repQueues =
        ReplicationFactory.getReplicationQueues(server.getZooKeeper(), conf, server);
    repQueues.init(server.getServerName().toString());
    final Path oldLogDir = new Path(TEST_UTIL.getDataTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    String fakeMachineName =
      URLEncoder.encode(server.getServerName().toString(), "UTF8");

    final FileSystem fs = FileSystem.get(conf);

    // Create 2 invalid files, 1 "recent" file, 1 very new file and 30 old files
    long now = System.currentTimeMillis();
    fs.delete(oldLogDir, true);
    fs.mkdirs(oldLogDir);
    // Case 1: 2 invalid files, which would be deleted directly
    fs.createNewFile(new Path(oldLogDir, "a"));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + "a"));
    // Case 2: 1 "recent" file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    System.out.println("Now is: " + now);
    for (int i = 1; i < 31; i++) {
      // Case 3: old files which would be deletable for the first log cleaner
      // (TimeToLiveLogCleaner), and also for the second (ReplicationLogCleaner)
      Path fileName = new Path(oldLogDir, fakeMachineName + "." + (now - i) );
      fs.createNewFile(fileName);
      // Case 4: put 3 old log files in ZK indicating that they are scheduled
      // for replication so these files would pass the first log cleaner
      // (TimeToLiveLogCleaner) but would be rejected by the second
      // (ReplicationLogCleaner)
      if (i % (30/3) == 1) {
        repQueues.addLog(fakeMachineName, fileName.getName());
        System.out.println("Replication log file: " + fileName);
      }
    }

    // sleep for sometime to get newer modifcation time
    Thread.sleep(ttl);
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + now));

    // Case 2: 1 newer file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now + 10000) ));

    for (FileStatus stat : fs.listStatus(oldLogDir)) {
      System.out.println(stat.getPath().toString());
    }

    assertEquals(34, fs.listStatus(oldLogDir).length);

    LogCleaner cleaner  = new LogCleaner(1000, server, conf, fs, oldLogDir);

    cleaner.chore();

    // We end up with the current log file, a newer one and the 3 old log
    // files which are scheduled for replication
    TEST_UTIL.waitFor(1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return 5 == fs.listStatus(oldLogDir).length;
      }
    });

    for (FileStatus file : fs.listStatus(oldLogDir)) {
      System.out.println("Kept log files: " + file.getPath().getName());
    }
  }

  @Test(timeout=5000)
  public void testZnodeCversionChange() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();
    cleaner.setConf(conf);

    ReplicationQueuesClient rqcMock = Mockito.mock(ReplicationQueuesClient.class);
    Mockito.when(rqcMock.getQueuesZNodeCversion()).thenReturn(1, 2, 3, 4);

    Field rqc = ReplicationLogCleaner.class.getDeclaredField("replicationQueues");
    rqc.setAccessible(true);

    rqc.set(cleaner, rqcMock);

    // This should return eventually when cversion stabilizes
    cleaner.getDeletableFiles(new LinkedList<FileStatus>());
  }

  @Test(timeout=10000)
  public void testZooKeeperAbortDuringGetListOfReplicators() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();

    List<FileStatus> dummyFiles = Lists.newArrayList(
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log1")),
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log2"))
    );

    FaultyZooKeeperWatcher faultyZK =
        new FaultyZooKeeperWatcher(conf, "testZooKeeperAbort-faulty", null);
    final AtomicBoolean getListOfReplicatorsFailed = new AtomicBoolean(false);

    try {
      faultyZK.init();
      ReplicationQueuesClient replicationQueuesClient = spy(ReplicationFactory.getReplicationQueuesClient(
        faultyZK, conf, new ReplicationLogCleaner.WarnOnlyAbortable()));
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          try {
            return invocation.callRealMethod();
          } catch (KeeperException.ConnectionLossException e) {
            getListOfReplicatorsFailed.set(true);
            throw e;
          }
        }
      }).when(replicationQueuesClient).getListOfReplicators();
      replicationQueuesClient.init();

      cleaner.setConf(conf, faultyZK, replicationQueuesClient);
      // should keep all files due to a ConnectionLossException getting the queues znodes
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
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testZooKeeperNormal() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();

    List<FileStatus> dummyFiles = Lists.newArrayList(
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log1")),
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log2"))
    );
    
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "testZooKeeperAbort-normal", null);
    try {
      cleaner.setConf(conf, zkw);
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
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(LogCleaner.OLD_WALS_CLEANER_SIZE, LogCleaner.OLD_WALS_CLEANER_DEFAULT_SIZE);
    // Prepare environments
    Server server = new DummyServer();
    Path oldWALsDir = new Path(TEST_UTIL.getDefaultRootDirPath(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    final LogCleaner cleaner = new LogCleaner(3000, server, conf, fs, oldWALsDir);
    assertEquals(LogCleaner.OLD_WALS_CLEANER_DEFAULT_SIZE, cleaner.getSizeOfCleaners());
    // Create dir and files for test
    fs.delete(oldWALsDir, true);
    fs.mkdirs(oldWALsDir);
    int numOfFiles = 10;
    createFiles(fs, oldWALsDir, numOfFiles);
    FileStatus[] status = fs.listStatus(oldWALsDir);
    assertEquals(numOfFiles, status.length);
    // Start cleaner chore
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        cleaner.chore();
      }
    });
    thread.setDaemon(true);
    thread.start();
    // change size of cleaners dynamically
    int sizeToChange = 4;
    conf.setInt(LogCleaner.OLD_WALS_CLEANER_SIZE, sizeToChange);
    cleaner.onConfigurationChange(conf);
    assertEquals(sizeToChange, cleaner.getSizeOfCleaners());
    // Stop chore
    thread.join();
    status = fs.listStatus(oldWALsDir);
    assertEquals(0, status.length);
  }

  private void createFiles(FileSystem fs, Path parentDir, int numOfFiles) throws IOException {
    Random random = new Random();
    for (int i = 0; i < numOfFiles; i++) {
      int xMega = 1 + random.nextInt(3); // size of each file is between 1~3M
      try (FSDataOutputStream fsdos = fs.create(new Path(parentDir, "file-" + i))) {
        for (int m = 0; m < xMega; m++) {
          byte[] M = new byte[1024 * 1024];
          random.nextBytes(M);
          fsdos.write(M);
        }
      }
    }
  }

  static class DummyServer implements Server {

    @Override
    public Configuration getConfiguration() {
      return TEST_UTIL.getConfiguration();
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
  }

  static class FaultyZooKeeperWatcher extends ZooKeeperWatcher {
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

    public RecoverableZooKeeper getRecoverableZooKeeper() {
      return zk;
    }
  }
}

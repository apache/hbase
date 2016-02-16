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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestLogsCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
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

  /**
   * ReplicationLogCleaner should be able to ride over ZooKeeper errors without
   * aborting.
   */
  @Test
  public void testZooKeeperAbort() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();

    List<FileStatus> dummyFiles = Lists.newArrayList(
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log1")),
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log2"))
    );

    FaultyZooKeeperWatcher faultyZK =
        new FaultyZooKeeperWatcher(conf, "testZooKeeperAbort-faulty", null);
    try {
      faultyZK.init();
      cleaner.setConf(conf, faultyZK);
      // should keep all files due to a ConnectionLossException getting the queues znodes
      Iterable<FileStatus> toDelete = cleaner.getDeletableFiles(dummyFiles);
      assertFalse(toDelete.iterator().hasNext());
      assertFalse(cleaner.isStopped());
    } finally {
      faultyZK.close();
    }

    // when zk is working both files should be returned
    cleaner = new ReplicationLogCleaner();
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
    public CatalogTracker getCatalogTracker() {
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
          .when(zk).getData("/hbase/replication/rs", null, new Stat());
    }

    public RecoverableZooKeeper getRecoverableZooKeeper() {
      return zk;
    }
  }
}


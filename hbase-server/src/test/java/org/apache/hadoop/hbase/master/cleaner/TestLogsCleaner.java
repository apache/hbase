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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesArguments;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClientZKImpl;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({MasterTests.class, MediumTests.class})
public class TestLogsCleaner {

  private static final Log LOG = LogFactory.getLog(TestLogsCleaner.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
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
    Configuration conf = TEST_UTIL.getConfiguration();
    // set TTLs
    long ttlWAL = 2000;
    long ttlProcedureWAL = 4000;
    conf.setLong("hbase.master.logcleaner.ttl", ttlWAL);
    conf.setLong("hbase.master.procedurewalcleaner.ttl", ttlProcedureWAL);

    Replication.decorateMasterConfiguration(conf);
    Server server = new DummyServer();
    ReplicationQueues repQueues = ReplicationFactory.getReplicationQueues(
        new ReplicationQueuesArguments(conf, server, server.getZooKeeper()));
    repQueues.init(server.getServerName().toString());
    final Path oldLogDir = new Path(TEST_UTIL.getDataTestDir(), HConstants.HREGION_OLDLOGDIR_NAME);
    final Path oldProcedureWALDir = new Path(oldLogDir, "masterProcedureWALs");
    String fakeMachineName = URLEncoder.encode(server.getServerName().toString(), "UTF8");

    final FileSystem fs = FileSystem.get(conf);

    long now = System.currentTimeMillis();
    fs.delete(oldLogDir, true);
    fs.mkdirs(oldLogDir);

    // Case 1: 2 invalid files, which would be deleted directly
    fs.createNewFile(new Path(oldLogDir, "a"));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + "a"));

    // Case 2: 5 Procedure WALs that are old which would be deleted
    for (int i = 1; i < 6; i++) {
      Path fileName = new Path(oldProcedureWALDir, String.format("pv-%020d.log", i));
      fs.createNewFile(fileName);
    }

    // Sleep for sometime to get old procedure WALs
    Thread.sleep(ttlProcedureWAL - ttlWAL);

    // Case 3: old WALs which would be deletable
    for (int i = 1; i < 31; i++) {
      Path fileName = new Path(oldLogDir, fakeMachineName + "." + (now - i));
      fs.createNewFile(fileName);
      // Case 4: put 3 WALs in ZK indicating that they are scheduled for replication so these
      // files would pass TimeToLiveLogCleaner but would be rejected by ReplicationLogCleaner
      if (i % (30 / 3) == 1) {
        repQueues.addLog(fakeMachineName, fileName.getName());
        LOG.info("Replication log file: " + fileName);
      }
    }

    // Case 5: 5 Procedure WALs that are new, will stay
    for (int i = 6; i < 11; i++) {
      Path fileName = new Path(oldProcedureWALDir, String.format("pv-%020d.log", i));
      fs.createNewFile(fileName);
    }

    // Sleep for sometime to get newer modification time
    Thread.sleep(ttlWAL);
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + now));

    // Case 6: 1 newer WAL, not even deletable for TimeToLiveLogCleaner,
    // so we are not going down the chain
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now + ttlWAL)));

    for (FileStatus stat : fs.listStatus(oldLogDir)) {
      LOG.info(stat.getPath().toString());
    }

    // There should be 34 files and masterProcedureWALs directory
    assertEquals(35, fs.listStatus(oldLogDir).length);
    // 10 procedure WALs
    assertEquals(10, fs.listStatus(oldProcedureWALDir).length);

    LogCleaner cleaner = new LogCleaner(1000, server, conf, fs, oldLogDir);
    cleaner.chore();

    // In oldWALs we end up with the current WAL, a newer WAL, the 3 old WALs which
    // are scheduled for replication and masterProcedureWALs directory
    TEST_UTIL.waitFor(1000,
        (Waiter.Predicate<Exception>) () -> 6 == fs.listStatus(oldLogDir).length);
    // In masterProcedureWALs we end up with 5 newer Procedure WALs
    TEST_UTIL.waitFor(1000,
        (Waiter.Predicate<Exception>) () -> 5 == fs.listStatus(oldProcedureWALDir).length);

    for (FileStatus file : fs.listStatus(oldLogDir)) {
      LOG.debug("Kept log file in oldWALs: " + file.getPath().getName());
    }
    for (FileStatus file : fs.listStatus(oldProcedureWALDir)) {
      LOG.debug("Kept log file in masterProcedureWALs: " + file.getPath().getName());
    }
  }

  @Test(timeout=5000)
  public void testZnodeCversionChange() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();
    cleaner.setConf(conf);

    ReplicationQueuesClientZKImpl rqcMock = Mockito.mock(ReplicationQueuesClientZKImpl.class);
    Mockito.when(rqcMock.getQueuesZNodeCversion()).thenReturn(1, 2, 3, 4);

    Field rqc = ReplicationLogCleaner.class.getDeclaredField("replicationQueues");
    rqc.setAccessible(true);

    rqc.set(cleaner, rqcMock);

    // This should return eventually when cversion stabilizes
    cleaner.getDeletableFiles(new LinkedList<>());
  }

  /**
   * ReplicationLogCleaner should be able to ride over ZooKeeper errors without aborting.
   */
  @Test
  public void testZooKeeperAbort() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    ReplicationLogCleaner cleaner = new ReplicationLogCleaner();

    List<FileStatus> dummyFiles = Lists.newArrayList(
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log1")),
        new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path("log2"))
    );

    try (FaultyZooKeeperWatcher faultyZK = new FaultyZooKeeperWatcher(conf,
        "testZooKeeperAbort-faulty", null)) {
      faultyZK.init();
      cleaner.setConf(conf, faultyZK);
      cleaner.preClean();
      // should keep all files due to a ConnectionLossException getting the queues znodes
      Iterable<FileStatus> toDelete = cleaner.getDeletableFiles(dummyFiles);
      assertFalse(toDelete.iterator().hasNext());
      assertFalse(cleaner.isStopped());
    }

    // when zk is working both files should be returned
    cleaner = new ReplicationLogCleaner();
    try (ZKWatcher zkw = new ZKWatcher(conf, "testZooKeeperAbort-normal", null)) {
      cleaner.setConf(conf, zkw);
      cleaner.preClean();
      Iterable<FileStatus> filesToDelete = cleaner.getDeletableFiles(dummyFiles);
      Iterator<FileStatus> iter = filesToDelete.iterator();
      assertTrue(iter.hasNext());
      assertEquals(new Path("log1"), iter.next().getPath());
      assertTrue(iter.hasNext());
      assertEquals(new Path("log2"), iter.next().getPath());
      assertFalse(iter.hasNext());
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

    @Override
    public ClusterConnection getClusterConnection() {
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
          .when(zk).getData("/hbase/replication/rs", null, new Stat());
    }

    public RecoverableZooKeeper getRecoverableZooKeeper() {
      return zk;
    }
  }
}

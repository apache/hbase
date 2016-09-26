/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.ReplicationQueuesZKImpl;
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MasterTests.class, SmallTests.class })
public class TestReplicationHFileCleaner {
  private static final Log LOG = LogFactory.getLog(ReplicationQueuesZKImpl.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Server server;
  private static ReplicationQueues rq;
  private static ReplicationPeers rp;
  private static final String peerId = "TestReplicationHFileCleaner";
  private static Configuration conf = TEST_UTIL.getConfiguration();
  static FileSystem fs = null;
  Path root;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    server = new DummyServer();
    conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    Replication.decorateMasterConfiguration(conf);
    rp = ReplicationFactory.getReplicationPeers(server.getZooKeeper(), conf, server);
    rp.init();

    rq = ReplicationFactory.getReplicationQueues(server.getZooKeeper(), conf, server);
    rq.init(server.getServerName().toString());
    try {
      fs = FileSystem.get(conf);
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Before
  public void setup() throws ReplicationException, IOException {
    root = TEST_UTIL.getDataTestDirOnTestFS();
    rp.addPeer(peerId, new ReplicationPeerConfig().setClusterKey(TEST_UTIL.getClusterKey()));
    rq.addPeerToHFileRefs(peerId);
  }

  @After
  public void cleanup() throws ReplicationException {
    try {
      fs.delete(root, true);
    } catch (IOException e) {
      LOG.warn("Failed to delete files recursively from path " + root);
    }
    rp.removePeer(peerId);
  }

  @Test
  public void testIsFileDeletable() throws IOException, ReplicationException {
    // 1. Create a file
    Path file = new Path(root, "testIsFileDeletableWithNoHFileRefs");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);
    // 3. Assert that file as is should be deletable
    assertTrue("Cleaner should allow to delete this file as there is no hfile reference node "
        + "for it in the queue.",
      cleaner.isFileDeletable(fs.getFileStatus(file)));

    List<String> files = new ArrayList<String>(1);
    files.add(file.getName());
    // 4. Add the file to hfile-refs queue
    rq.addHFileRefs(peerId, files);
    // 5. Assert file should not be deletable
    assertFalse("Cleaner should not allow to delete this file as there is a hfile reference node "
        + "for it in the queue.",
      cleaner.isFileDeletable(fs.getFileStatus(file)));
  }

  @Test
  public void testGetDeletableFiles() throws Exception {
    // 1. Create two files and assert that they do not exist
    Path notDeletablefile = new Path(root, "testGetDeletableFiles_1");
    fs.createNewFile(notDeletablefile);
    assertTrue("Test file not created!", fs.exists(notDeletablefile));
    Path deletablefile = new Path(root, "testGetDeletableFiles_2");
    fs.createNewFile(deletablefile);
    assertTrue("Test file not created!", fs.exists(deletablefile));

    List<FileStatus> files = new ArrayList<FileStatus>(2);
    FileStatus f = new FileStatus();
    f.setPath(deletablefile);
    files.add(f);
    f = new FileStatus();
    f.setPath(notDeletablefile);
    files.add(f);

    List<String> hfiles = new ArrayList<>(1);
    hfiles.add(notDeletablefile.getName());
    // 2. Add one file to hfile-refs queue
    rq.addHFileRefs(peerId, hfiles);

    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);
    Iterator<FileStatus> deletableFilesIterator = cleaner.getDeletableFiles(files).iterator();
    int i = 0;
    while (deletableFilesIterator.hasNext() && i < 2) {
      i++;
    }
    // 5. Assert one file should not be deletable and it is present in the list returned
    if (i > 2) {
      fail("File " + notDeletablefile
          + " should not be deletable as its hfile reference node is not added.");
    }
    assertTrue(deletableFilesIterator.next().getPath().equals(deletablefile));
  }

  /*
   * Test for HBASE-14621. This test will not assert directly anything. Without the fix the test
   * will end up in a infinite loop, so it will timeout.
   */
  @Test(timeout = 15000)
  public void testForDifferntHFileRefsZnodeVersion() throws Exception {
    // 1. Create a file
    Path file = new Path(root, "testForDifferntHFileRefsZnodeVersion");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);

    ReplicationQueuesClient replicationQueuesClient = Mockito.mock(ReplicationQueuesClient.class);
    //Return different znode version for each call
    Mockito.when(replicationQueuesClient.getHFileRefsNodeChangeVersion()).thenReturn(1, 2);

    Class<? extends ReplicationHFileCleaner> cleanerClass = cleaner.getClass();
    Field rqc = cleanerClass.getDeclaredField("rqc");
    rqc.setAccessible(true);
    rqc.set(cleaner, replicationQueuesClient);

    cleaner.isFileDeletable(fs.getFileStatus(file));
  }

  /**
   * ReplicationHFileCleaner should be able to ride over ZooKeeper errors without aborting.
   */
  @Test
  public void testZooKeeperAbort() throws Exception {
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();

    List<FileStatus> dummyFiles =
        Lists.newArrayList(new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path(
            "hfile1")), new FileStatus(100, false, 3, 100, System.currentTimeMillis(), new Path(
            "hfile2")));

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
    cleaner = new ReplicationHFileCleaner();
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "testZooKeeperAbort-normal", null);
    try {
      cleaner.setConf(conf, zkw);
      Iterable<FileStatus> filesToDelete = cleaner.getDeletableFiles(dummyFiles);
      Iterator<FileStatus> iter = filesToDelete.iterator();
      assertTrue(iter.hasNext());
      assertEquals(new Path("hfile1"), iter.next().getPath());
      assertTrue(iter.hasNext());
      assertEquals(new Path("hfile2"), iter.next().getPath());
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

  static class FaultyZooKeeperWatcher extends ZooKeeperWatcher {
    private RecoverableZooKeeper zk;
    public FaultyZooKeeperWatcher(Configuration conf, String identifier, Abortable abortable)
        throws ZooKeeperConnectionException, IOException {
      super(conf, identifier, abortable);
    }

    public void init() throws Exception {
      this.zk = spy(super.getRecoverableZooKeeper());
      doThrow(new KeeperException.ConnectionLossException())
          .when(zk).getData("/hbase/replication/hfile-refs", null, new Stat());
    }

    public RecoverableZooKeeper getRecoverableZooKeeper() {
      return zk;
    }
  }
}
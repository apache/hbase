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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.keymeta.KeymetaAdmin;
import org.apache.hadoop.hbase.keymeta.ManagedKeyAccessor;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.MockServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ MasterTests.class, SmallTests.class })
public class TestReplicationHFileCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationHFileCleaner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationHFileCleaner.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Server server;
  private static final TableName tableName = TableName.valueOf("test_cleaner");
  private static ReplicationQueueStorage rq;
  private static ReplicationPeers rp;
  private static final String peerId = "TestReplicationHFileCleaner";
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static FileSystem fs = null;
  private static Map<String, Object> params;
  private Path root;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    server = new DummyServer();
    params = ImmutableMap.of(HMaster.MASTER, server);
    conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    HMaster.decorateMasterConfiguration(conf);
    TableDescriptor td = ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName);
    TEST_UTIL.getAdmin().createTable(td);
    conf.set(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME, tableName.getNameAsString());
    rp =
      ReplicationFactory.getReplicationPeers(server.getFileSystem(), server.getZooKeeper(), conf);
    rp.init();
    rq = ReplicationStorageFactory.getReplicationQueueStorage(server.getConnection(), conf);
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws ReplicationException, IOException {
    root = TEST_UTIL.getDataTestDirOnTestFS();
    rp.getPeerStorage().addPeer(peerId,
      ReplicationPeerConfig.newBuilder().setClusterKey(TEST_UTIL.getRpcConnnectionURI()).build(),
      true, SyncReplicationState.NONE);
  }

  @After
  public void cleanup() throws ReplicationException {
    try {
      fs.delete(root, true);
    } catch (IOException e) {
      LOG.warn("Failed to delete files recursively from path " + root);
    }
    // Remove all HFileRefs (if any)
    rq.removeHFileRefs(peerId, rq.getReplicableHFiles(peerId));
    rp.getPeerStorage().removePeer(peerId);
  }

  private ReplicationHFileCleaner createCleaner() {
    ReplicationHFileCleaner cleaner = new ReplicationHFileCleaner();
    cleaner.setConf(conf);
    cleaner.init(params);
    return cleaner;
  }

  @Test
  public void testIsFileDeletable() throws IOException, ReplicationException {
    // 1. Create a file
    Path file = new Path(root, "testIsFileDeletableWithNoHFileRefs");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    ReplicationHFileCleaner cleaner = createCleaner();
    // 3. Assert that file as is should be deletable
    assertTrue("Cleaner should allow to delete this file as there is no hfile reference node "
      + "for it in the queue.", cleaner.isFileDeletable(fs.getFileStatus(file)));

    List<Pair<Path, Path>> files = new ArrayList<>(1);
    files.add(new Pair<>(null, file));
    // 4. Add the file to hfile-refs queue
    rq.addHFileRefs(peerId, files);
    // 5. Assert file should not be deletable
    assertFalse("Cleaner should not allow to delete this file as there is a hfile reference node "
      + "for it in the queue.", cleaner.isFileDeletable(fs.getFileStatus(file)));
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

    List<FileStatus> files = new ArrayList<>(2);
    FileStatus f = new FileStatus();
    f.setPath(deletablefile);
    files.add(f);
    f = new FileStatus();
    f.setPath(notDeletablefile);
    files.add(f);

    List<Pair<Path, Path>> hfiles = new ArrayList<>(1);
    hfiles.add(new Pair<>(null, notDeletablefile));
    // 2. Add one file to hfile-refs queue
    rq.addHFileRefs(peerId, hfiles);

    ReplicationHFileCleaner cleaner = createCleaner();
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

  static class DummyServer extends MockServer {

    @Override
    public Configuration getConfiguration() {
      return TEST_UTIL.getConfiguration();
    }

    @Override
    public ZKWatcher getZooKeeper() {
      try {
        return TEST_UTIL.getZooKeeperWatcher();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public Connection getConnection() {
      try {
        return TEST_UTIL.getConnection();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override public SystemKeyCache getSystemKeyCache() {
      return null;
    }

    @Override public ManagedKeyAccessor getManagedKeyAccessor() {
      return null;
    }

    @Override public KeymetaAdmin getKeymetaAdmin() {
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      try {
        return TEST_UTIL.getTestFileSystem();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}

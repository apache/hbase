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
package org.apache.hadoop.hbase.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.ReplicationPeer;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.Table.State;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileV1Detector;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Upgrade to 0.96 involves detecting HFileV1 in existing cluster, updating namespace and
 * updating znodes. This class tests for HFileV1 detection and upgrading znodes.
 * Uprading namespace is tested in {@link TestNamespaceUpgrade}.
 */
@Category(MediumTests.class)
public class TestUpgradeTo96 {

  static final Log LOG = LogFactory.getLog(TestUpgradeTo96.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * underlying file system instance
   */
  private static FileSystem fs;
  /**
   * hbase root dir
   */
  private static Path hbaseRootDir;
  private static ZooKeeperWatcher zkw;
  /**
   * replication peer znode (/hbase/replication/peers)
   */
  private static String replicationPeerZnode;
  /**
   * znode of a table
   */
  private static String tableAZnode;
  private static ReplicationPeer peer1;
  /**
   * znode for replication peer1 (/hbase/replication/peers/1)
   */
  private static String peer1Znode;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start up the mini cluster on top of an 0.94 root.dir that has data from
    // a 0.94 hbase run and see if we can migrate to 0.96
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);

    hbaseRootDir = TEST_UTIL.getDefaultRootDirPath();
    fs = FileSystem.get(TEST_UTIL.getConfiguration());
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), hbaseRootDir);
    zkw = TEST_UTIL.getZooKeeperWatcher();

    Path testdir = TEST_UTIL.getDataTestDir("TestUpgradeTo96");
    // get the untar 0.94 file structure

    set94FSLayout(testdir);
    setUp94Znodes();
  }

  /**
   * Lays out 0.94 file system layout using {@link TestNamespaceUpgrade} apis.
   * @param testdir
   * @throws IOException
   * @throws Exception
   */
  private static void set94FSLayout(Path testdir) throws IOException, Exception {
    File untar = TestNamespaceUpgrade.untar(new File(testdir.toString()));
    if (!fs.exists(hbaseRootDir.getParent())) {
      // mkdir at first
      fs.mkdirs(hbaseRootDir.getParent());
    }
    FsShell shell = new FsShell(TEST_UTIL.getConfiguration());
    shell.run(new String[] { "-put", untar.toURI().toString(), hbaseRootDir.toString() });
    // See whats in minihdfs.
    shell.run(new String[] { "-lsr", "/" });
  }

  /**
   * Sets znodes used in 0.94 version. Only table and replication znodes will be upgraded to PB,
   * others would be deleted.
   * @throws KeeperException
   */
  private static void setUp94Znodes() throws IOException, KeeperException {
    // add some old znodes, which would be deleted after upgrade.
    String rootRegionServerZnode = ZKUtil.joinZNode(zkw.baseZNode, "root-region-server");
    ZKUtil.createWithParents(zkw, rootRegionServerZnode);
    ZKUtil.createWithParents(zkw, zkw.backupMasterAddressesZNode);
    // add table znode, data of its children would be protobuffized
    tableAZnode = ZKUtil.joinZNode(zkw.tableZNode, "a");
    ZKUtil.createWithParents(zkw, tableAZnode,
      Bytes.toBytes(ZooKeeperProtos.Table.State.ENABLED.toString()));
    // add replication znodes, data of its children would be protobuffized
    String replicationZnode = ZKUtil.joinZNode(zkw.baseZNode, "replication");
    replicationPeerZnode = ZKUtil.joinZNode(replicationZnode, "peers");
    peer1Znode = ZKUtil.joinZNode(replicationPeerZnode, "1");
    peer1 = ReplicationPeer.newBuilder().setClusterkey("abc:123:/hbase").build();
    ZKUtil.createWithParents(zkw, peer1Znode, Bytes.toBytes(peer1.getClusterkey()));
  }

  /**
   * Tests a 0.94 filesystem for any HFileV1.
   * @throws Exception
   */
  @Test
  public void testHFileV1Detector() throws Exception {
    assertEquals(0, ToolRunner.run(TEST_UTIL.getConfiguration(), new HFileV1Detector(), null));
  }

  /**
   * Creates a corrupt file, and run HFileV1 detector tool
   * @throws Exception
   */
  @Test
  public void testHFileV1DetectorWithCorruptFiles() throws Exception {
    // add a corrupt file.
    Path tablePath = new Path(hbaseRootDir, "foo");
    FileStatus[] regionsDir = fs.listStatus(tablePath);
    if (regionsDir == null) throw new IOException("No Regions found for table " + "foo");
    Path columnFamilyDir = null;
    Path targetRegion = null;
    for (FileStatus s : regionsDir) {
      if (fs.exists(new Path(s.getPath(), HRegionFileSystem.REGION_INFO_FILE))) {
        targetRegion = s.getPath();
        break;
      }
    }
    FileStatus[] cfs = fs.listStatus(targetRegion);
    for (FileStatus f : cfs) {
      if (f.isDirectory()) {
        columnFamilyDir = f.getPath();
        break;
      }
    }
    LOG.debug("target columnFamilyDir: " + columnFamilyDir);
    // now insert a corrupt file in the columnfamily.
    Path corruptFile = new Path(columnFamilyDir, "corrupt_file");
    if (!fs.createNewFile(corruptFile)) throw new IOException("Couldn't create corrupt file: "
        + corruptFile);
    assertEquals(1, ToolRunner.run(TEST_UTIL.getConfiguration(), new HFileV1Detector(), null));
    // remove the corrupt file
    FileSystem.get(TEST_UTIL.getConfiguration()).delete(corruptFile, false);
  }

  @Test
  public void testHFileLink() throws Exception {
    // pass a link, and verify that correct paths are returned.
    Path rootDir = FSUtils.getRootDir(TEST_UTIL.getConfiguration());
    Path aFileLink = new Path(rootDir, "table/2086db948c48/cf/table=21212abcdc33-0906db948c48");
    Path preNamespaceTablePath = new Path(rootDir, "table/21212abcdc33/cf/0906db948c48");
    Path preNamespaceArchivePath =
      new Path(rootDir, ".archive/table/21212abcdc33/cf/0906db948c48");
    Path preNamespaceTempPath = new Path(rootDir, ".tmp/table/21212abcdc33/cf/0906db948c48");
    boolean preNSTablePathExists = false;
    boolean preNSArchivePathExists = false;
    boolean preNSTempPathExists = false;
    assertTrue(HFileLink.isHFileLink(aFileLink));
    HFileLink hFileLink = new HFileLink(TEST_UTIL.getConfiguration(), aFileLink);
    assertTrue(hFileLink.getArchivePath().toString().startsWith(rootDir.toString()));

    HFileV1Detector t = new HFileV1Detector();
    t.setConf(TEST_UTIL.getConfiguration());
    FileLink fileLink = t.getFileLinkWithPreNSPath(aFileLink);
    //assert it has 6 paths (2 NS, 2 Pre NS, and 2 .tmp)  to look.
    assertTrue(fileLink.getLocations().length == 6);
    for (Path p : fileLink.getLocations()) {
      if (p.equals(preNamespaceArchivePath)) preNSArchivePathExists = true;
      if (p.equals(preNamespaceTablePath)) preNSTablePathExists = true;
      if (p.equals(preNamespaceTempPath)) preNSTempPathExists = true;
    }
    assertTrue(preNSArchivePathExists & preNSTablePathExists & preNSTempPathExists);
  }

  @Test
  public void testADirForHFileV1() throws Exception {
    Path tablePath = new Path(hbaseRootDir, "foo");
    System.out.println("testADirForHFileV1: " + tablePath.makeQualified(fs));
    System.out.println("Passed: " + hbaseRootDir + "/foo");
    assertEquals(0,
      ToolRunner.run(TEST_UTIL.getConfiguration(), new HFileV1Detector(), new String[] { "-p"
          + "foo" }));
  }

  @Test
  public void testZnodeMigration() throws Exception {
    String rootRSZnode = ZKUtil.joinZNode(zkw.baseZNode, "root-region-server");
    assertTrue(ZKUtil.checkExists(zkw, rootRSZnode) > -1);
    ToolRunner.run(TEST_UTIL.getConfiguration(), new UpgradeTo96(), new String[] { "-execute" });
    assertEquals(-1, ZKUtil.checkExists(zkw, rootRSZnode));
    byte[] data = ZKUtil.getData(zkw, tableAZnode);
    assertTrue(ProtobufUtil.isPBMagicPrefix(data));
    checkTableState(data, ZooKeeperProtos.Table.State.ENABLED);
    // ensure replication znodes are there, and protobuffed.
    data = ZKUtil.getData(zkw, peer1Znode);
    assertTrue(ProtobufUtil.isPBMagicPrefix(data));
    checkReplicationPeerData(data, peer1);
  }

  private void checkTableState(byte[] data, State expectedState)
      throws InvalidProtocolBufferException {
    ZooKeeperProtos.Table.Builder builder = ZooKeeperProtos.Table.newBuilder();
    int magicLen = ProtobufUtil.lengthOfPBMagic();
    ZooKeeperProtos.Table t = builder.mergeFrom(data, magicLen, data.length - magicLen).build();
    assertTrue(t.getState() == expectedState);
  }

  private void checkReplicationPeerData(byte[] data, ReplicationPeer peer)
      throws InvalidProtocolBufferException {
    int magicLen = ProtobufUtil.lengthOfPBMagic();
    ZooKeeperProtos.ReplicationPeer.Builder builder = ZooKeeperProtos.ReplicationPeer.newBuilder();
    assertEquals(builder.mergeFrom(data, magicLen, data.length - magicLen).build().getClusterkey(),
      peer.getClusterkey());

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
    TEST_UTIL.shutdownMiniZKCluster();
  }

}

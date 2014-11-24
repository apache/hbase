/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests invocation of the
 * {@link org.apache.hadoop.hbase.coprocessor.MasterObserver} interface hooks at
 * all appropriate times during normal HMaster operations.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestWALObserver {
  private static final Log LOG = LogFactory.getLog(TestWALObserver.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[] TEST_TABLE = Bytes.toBytes("observedTable");
  private static byte[][] TEST_FAMILY = { Bytes.toBytes("fam1"),
      Bytes.toBytes("fam2"), Bytes.toBytes("fam3"), };
  private static byte[][] TEST_QUALIFIER = { Bytes.toBytes("q1"),
      Bytes.toBytes("q2"), Bytes.toBytes("q3"), };
  private static byte[][] TEST_VALUE = { Bytes.toBytes("v1"),
      Bytes.toBytes("v2"), Bytes.toBytes("v3"), };
  private static byte[] TEST_ROW = Bytes.toBytes("testRow");

  private Configuration conf;
  private FileSystem fs;
  private Path dir;
  private Path hbaseRootDir;
  private String logName;
  private Path oldLogDir;
  private Path logDir;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    conf.setBoolean("dfs.support.append", true);
    conf.setInt("dfs.client.block.recovery.retries", 2);

    TEST_UTIL.startMiniCluster(1);
    Path hbaseRootDir = TEST_UTIL.getDFSCluster().getFileSystem()
        .makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    FSUtils.setRootDir(conf, hbaseRootDir);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    // this.cluster = TEST_UTIL.getDFSCluster();
    this.fs = TEST_UTIL.getDFSCluster().getFileSystem();
    this.hbaseRootDir = FSUtils.getRootDir(conf);
    this.dir = new Path(this.hbaseRootDir, TestWALObserver.class.getName());
    this.oldLogDir = new Path(this.hbaseRootDir,
        HConstants.HREGION_OLDLOGDIR_NAME);
    this.logDir = new Path(this.hbaseRootDir, HConstants.HREGION_LOGDIR_NAME);
    this.logName = HConstants.HREGION_LOGDIR_NAME;

    if (TEST_UTIL.getDFSCluster().getFileSystem().exists(this.hbaseRootDir)) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
    }
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
  }

  /**
   * Test WAL write behavior with WALObserver. The coprocessor monitors a
   * WALEdit written to WAL, and ignore, modify, and add KeyValue's for the
   * WALEdit.
   */
  @Test
  public void testWALObserverWriteToWAL() throws Exception {

    HRegionInfo hri = createBasic3FamilyHRegionInfo(Bytes.toString(TEST_TABLE));
    final HTableDescriptor htd = createBasic3FamilyHTD(Bytes
        .toString(TEST_TABLE));

    Path basedir = new Path(this.hbaseRootDir, Bytes.toString(TEST_TABLE));
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));
    final AtomicLong sequenceId = new AtomicLong(0);

    HLog log = HLogFactory.createHLog(this.fs, hbaseRootDir,
        TestWALObserver.class.getName(), this.conf);
    SampleRegionWALObserver cp = getCoprocessor(log);

    // TEST_FAMILY[0] shall be removed from WALEdit.
    // TEST_FAMILY[1] value shall be changed.
    // TEST_FAMILY[2] shall be added to WALEdit, although it's not in the put.
    cp.setTestValues(TEST_TABLE, TEST_ROW, TEST_FAMILY[0], TEST_QUALIFIER[0],
        TEST_FAMILY[1], TEST_QUALIFIER[1], TEST_FAMILY[2], TEST_QUALIFIER[2]);

    assertFalse(cp.isPreWALWriteCalled());
    assertFalse(cp.isPostWALWriteCalled());

    // TEST_FAMILY[2] is not in the put, however it shall be added by the tested
    // coprocessor.
    // Use a Put to create familyMap.
    Put p = creatPutWith2Families(TEST_ROW);

    Map<byte[], List<Cell>> familyMap = p.getFamilyCellMap();
    WALEdit edit = new WALEdit();
    addFamilyMapToWALEdit(familyMap, edit);

    boolean foundFamily0 = false;
    boolean foundFamily2 = false;
    boolean modifiedFamily1 = false;

    List<Cell> cells = edit.getCells();

    for (Cell cell : cells) {
      if (Arrays.equals(cell.getFamily(), TEST_FAMILY[0])) {
        foundFamily0 = true;
      }
      if (Arrays.equals(cell.getFamily(), TEST_FAMILY[2])) {
        foundFamily2 = true;
      }
      if (Arrays.equals(cell.getFamily(), TEST_FAMILY[1])) {
        if (!Arrays.equals(cell.getValue(), TEST_VALUE[1])) {
          modifiedFamily1 = true;
        }
      }
    }
    assertTrue(foundFamily0);
    assertFalse(foundFamily2);
    assertFalse(modifiedFamily1);

    // it's where WAL write cp should occur.
    long now = EnvironmentEdgeManager.currentTime();
    log.append(hri, hri.getTable(), edit, now, htd, sequenceId);

    // the edit shall have been change now by the coprocessor.
    foundFamily0 = false;
    foundFamily2 = false;
    modifiedFamily1 = false;
    for (Cell cell : cells) {
      if (Arrays.equals(cell.getFamily(), TEST_FAMILY[0])) {
        foundFamily0 = true;
      }
      if (Arrays.equals(cell.getFamily(), TEST_FAMILY[2])) {
        foundFamily2 = true;
      }
      if (Arrays.equals(cell.getFamily(), TEST_FAMILY[1])) {
        if (!Arrays.equals(cell.getValue(), TEST_VALUE[1])) {
          modifiedFamily1 = true;
        }
      }
    }
    assertFalse(foundFamily0);
    assertTrue(foundFamily2);
    assertTrue(modifiedFamily1);

    assertTrue(cp.isPreWALWriteCalled());
    assertTrue(cp.isPostWALWriteCalled());
  }

  /**
   * Test WAL replay behavior with WALObserver.
   */
  @Test
  public void testWALCoprocessorReplay() throws Exception {
    // WAL replay is handled at HRegion::replayRecoveredEdits(), which is
    // ultimately called by HRegion::initialize()
    TableName tableName = TableName.valueOf("testWALCoprocessorReplay");
    final HTableDescriptor htd = getBasic3FamilyHTableDescriptor(tableName);
    final AtomicLong sequenceId = new AtomicLong(0);
    // final HRegionInfo hri =
    // createBasic3FamilyHRegionInfo(Bytes.toString(tableName));
    // final HRegionInfo hri1 =
    // createBasic3FamilyHRegionInfo(Bytes.toString(tableName));
    final HRegionInfo hri = new HRegionInfo(tableName, null, null);

    final Path basedir =
        FSUtils.getTableDir(this.hbaseRootDir, tableName);
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));

    final Configuration newConf = HBaseConfiguration.create(this.conf);

    // HLog wal = new HLog(this.fs, this.dir, this.oldLogDir, this.conf);
    HLog wal = createWAL(this.conf);
    // Put p = creatPutWith2Families(TEST_ROW);
    WALEdit edit = new WALEdit();
    long now = EnvironmentEdgeManager.currentTime();
    // addFamilyMapToWALEdit(p.getFamilyMap(), edit);
    final int countPerFamily = 1000;
    // for (HColumnDescriptor hcd: hri.getTableDesc().getFamilies()) {
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      // addWALEdits(tableName, hri, TEST_ROW, hcd.getName(), countPerFamily,
      // EnvironmentEdgeManager.getDelegate(), wal);
      addWALEdits(tableName, hri, TEST_ROW, hcd.getName(), countPerFamily,
          EnvironmentEdgeManager.getDelegate(), wal, htd, sequenceId);
    }
    wal.append(hri, tableName, edit, now, htd, sequenceId);
    // sync to fs.
    wal.sync();

    User user = HBaseTestingUtility.getDifferentUser(newConf,
        ".replay.wal.secondtime");
    user.runAs(new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Path p = runWALSplit(newConf);
        LOG.info("WALSplit path == " + p);
        FileSystem newFS = FileSystem.get(newConf);
        // Make a new wal for new region open.
        HLog wal2 = createWAL(newConf);
        HRegion region = HRegion.openHRegion(newConf, FileSystem.get(newConf), hbaseRootDir,
            hri, htd, wal2, TEST_UTIL.getHBaseCluster().getRegionServer(0), null);
        long seqid2 = region.getOpenSeqNum();

        SampleRegionWALObserver cp2 =
          (SampleRegionWALObserver)region.getCoprocessorHost().findCoprocessor(
              SampleRegionWALObserver.class.getName());
        // TODO: asserting here is problematic.
        assertNotNull(cp2);
        assertTrue(cp2.isPreWALRestoreCalled());
        assertTrue(cp2.isPostWALRestoreCalled());
        region.close();
        wal2.closeAndDelete();
        return null;
      }
    });
  }

  /**
   * Test to see CP loaded successfully or not. There is a duplication at
   * TestHLog, but the purpose of that one is to see whether the loaded CP will
   * impact existing HLog tests or not.
   */
  @Test
  public void testWALObserverLoaded() throws Exception {
    HLog log = HLogFactory.createHLog(fs, hbaseRootDir,
        TestWALObserver.class.getName(), conf);
    assertNotNull(getCoprocessor(log));
  }

  private SampleRegionWALObserver getCoprocessor(HLog wal) throws Exception {
    WALCoprocessorHost host = wal.getCoprocessorHost();
    Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class
        .getName());
    return (SampleRegionWALObserver) c;
  }

  /*
   * Creates an HRI around an HTD that has <code>tableName</code> and three
   * column families named.
   * 
   * @param tableName Name of table to use when we create HTableDescriptor.
   */
  private HRegionInfo createBasic3FamilyHRegionInfo(final String tableName) {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

    for (int i = 0; i < TEST_FAMILY.length; i++) {
      HColumnDescriptor a = new HColumnDescriptor(TEST_FAMILY[i]);
      htd.addFamily(a);
    }
    return new HRegionInfo(htd.getTableName(), null, null, false);
  }

  /*
   * @param p Directory to cleanup
   */
  private void deleteDir(final Path p) throws IOException {
    if (this.fs.exists(p)) {
      if (!this.fs.delete(p, true)) {
        throw new IOException("Failed remove of " + p);
      }
    }
  }

  private Put creatPutWith2Families(byte[] row) throws IOException {
    Put p = new Put(row);
    for (int i = 0; i < TEST_FAMILY.length - 1; i++) {
      p.add(TEST_FAMILY[i], TEST_QUALIFIER[i], TEST_VALUE[i]);
    }
    return p;
  }

  /**
   * Copied from HRegion.
   * 
   * @param familyMap
   *          map of family->edits
   * @param walEdit
   *          the destination entry to append into
   */
  private void addFamilyMapToWALEdit(Map<byte[], List<Cell>> familyMap,
      WALEdit walEdit) {
    for (List<Cell> edits : familyMap.values()) {
      for (Cell cell : edits) {
        // KeyValue v1 expectation. Cast for now until we go all Cell all the time. TODO.
        walEdit.add(cell);
      }
    }
  }

  private Path runWALSplit(final Configuration c) throws IOException {
    List<Path> splits = HLogSplitter.split(
      hbaseRootDir, logDir, oldLogDir, FileSystem.get(c), c);
    // Split should generate only 1 file since there's only 1 region
    assertEquals(1, splits.size());
    // Make sure the file exists
    assertTrue(fs.exists(splits.get(0)));
    LOG.info("Split file=" + splits.get(0));
    return splits.get(0);
  }

  private HLog createWAL(final Configuration c) throws IOException {
    return HLogFactory.createHLog(FileSystem.get(c), hbaseRootDir, logName, c);
  }

  private void addWALEdits(final TableName tableName, final HRegionInfo hri,
      final byte[] rowName, final byte[] family, final int count,
      EnvironmentEdge ee, final HLog wal, final HTableDescriptor htd, final AtomicLong sequenceId)
      throws IOException {
    String familyStr = Bytes.toString(family);
    for (int j = 0; j < count; j++) {
      byte[] qualifierBytes = Bytes.toBytes(Integer.toString(j));
      byte[] columnBytes = Bytes.toBytes(familyStr + ":" + Integer.toString(j));
      WALEdit edit = new WALEdit();
      edit.add(new KeyValue(rowName, family, qualifierBytes, ee.currentTime(), columnBytes));
      wal.append(hri, tableName, edit, ee.currentTime(), htd, sequenceId);
    }
  }

  private HTableDescriptor getBasic3FamilyHTableDescriptor(
      final TableName tableName) {
    HTableDescriptor htd = new HTableDescriptor(tableName);

    for (int i = 0; i < TEST_FAMILY.length; i++) {
      HColumnDescriptor a = new HColumnDescriptor(TEST_FAMILY[i]);
      htd.addFamily(a);
    }
    return htd;
  }

  private HTableDescriptor createBasic3FamilyHTD(final String tableName) {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor a = new HColumnDescriptor(Bytes.toBytes("a"));
    htd.addFamily(a);
    HColumnDescriptor b = new HColumnDescriptor(Bytes.toBytes("b"));
    htd.addFamily(b);
    HColumnDescriptor c = new HColumnDescriptor(Bytes.toBytes("c"));
    htd.addFamily(c);
    return htd;
  }
}

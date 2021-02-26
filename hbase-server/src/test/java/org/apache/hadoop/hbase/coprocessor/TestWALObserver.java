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
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests invocation of the
 * {@link org.apache.hadoop.hbase.coprocessor.MasterObserver} interface hooks at
 * all appropriate times during normal HMaster operations.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestWALObserver {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALObserver.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALObserver.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[] TEST_TABLE = Bytes.toBytes("observedTable");
  private static byte[][] TEST_FAMILY = { Bytes.toBytes("fam1"),
      Bytes.toBytes("fam2"), Bytes.toBytes("fam3"), };
  private static byte[][] TEST_QUALIFIER = { Bytes.toBytes("q1"),
      Bytes.toBytes("q2"), Bytes.toBytes("q3"), };
  private static byte[][] TEST_VALUE = { Bytes.toBytes("v1"),
      Bytes.toBytes("v2"), Bytes.toBytes("v3"), };
  private static byte[] TEST_ROW = Bytes.toBytes("testRow");

  @Rule
  public TestName currentTest = new TestName();

  private Configuration conf;
  private FileSystem fs;
  private Path hbaseRootDir;
  private Path hbaseWALRootDir;
  private Path oldLogDir;
  private Path logDir;
  private WALFactory wals;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALCoprocessor.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        SampleRegionWALCoprocessor.class.getName());
    conf.setInt("dfs.client.block.recovery.retries", 2);

    TEST_UTIL.startMiniCluster(1);
    Path hbaseRootDir = TEST_UTIL.getDFSCluster().getFileSystem()
        .makeQualified(new Path("/hbase"));
    Path hbaseWALRootDir = TEST_UTIL.getDFSCluster().getFileSystem()
            .makeQualified(new Path("/hbaseLogRoot"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    CommonFSUtils.setRootDir(conf, hbaseRootDir);
    CommonFSUtils.setWALRootDir(conf, hbaseWALRootDir);
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
    this.hbaseRootDir = CommonFSUtils.getRootDir(conf);
    this.hbaseWALRootDir = CommonFSUtils.getWALRootDir(conf);
    this.oldLogDir = new Path(this.hbaseWALRootDir,
        HConstants.HREGION_OLDLOGDIR_NAME);
    String serverName = ServerName.valueOf(currentTest.getMethodName(), 16010,
        System.currentTimeMillis()).toString();
    this.logDir = new Path(this.hbaseWALRootDir,
      AbstractFSWALProvider.getWALDirectoryName(serverName));

    if (TEST_UTIL.getDFSCluster().getFileSystem().exists(this.hbaseRootDir)) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
    }
    if (TEST_UTIL.getDFSCluster().getFileSystem().exists(this.hbaseWALRootDir)) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseWALRootDir, true);
    }
    this.wals = new WALFactory(conf, serverName);
  }

  @After
  public void tearDown() throws Exception {
    try {
      wals.shutdown();
    } catch (IOException exception) {
      // one of our tests splits out from under our wals.
      LOG.warn("Ignoring failure to close wal factory. " + exception.getMessage());
      LOG.debug("details of failure to close wal factory.", exception);
    }
    TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
    TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseWALRootDir, true);
  }

  /**
   * Test WAL write behavior with WALObserver. The coprocessor monitors a
   * WALEdit written to WAL, and ignore, modify, and add KeyValue's for the
   * WALEdit.
   */
  @Test
  public void testWALObserverWriteToWAL() throws Exception {
    final WAL log = wals.getWAL(null);
    verifyWritesSeen(log, getCoprocessor(log, SampleRegionWALCoprocessor.class), false);
  }

  private void verifyWritesSeen(final WAL log, final SampleRegionWALCoprocessor cp,
      final boolean seesLegacy) throws Exception {
    RegionInfo hri = createBasicHRegionInfo(Bytes.toString(TEST_TABLE));
    TableDescriptor htd = createBasic3FamilyHTD(Bytes
        .toString(TEST_TABLE));
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    Path basedir = new Path(this.hbaseRootDir, Bytes.toString(TEST_TABLE));
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));

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
    edit.add(familyMap);

    boolean foundFamily0 = false;
    boolean foundFamily2 = false;
    boolean modifiedFamily1 = false;

    List<Cell> cells = edit.getCells();

    for (Cell cell : cells) {
      if (Arrays.equals(CellUtil.cloneFamily(cell), TEST_FAMILY[0])) {
        foundFamily0 = true;
      }
      if (Arrays.equals(CellUtil.cloneFamily(cell), TEST_FAMILY[2])) {
        foundFamily2 = true;
      }
      if (Arrays.equals(CellUtil.cloneFamily(cell), TEST_FAMILY[1])) {
        if (!Arrays.equals(CellUtil.cloneValue(cell), TEST_VALUE[1])) {
          modifiedFamily1 = true;
        }
      }
    }
    assertTrue(foundFamily0);
    assertFalse(foundFamily2);
    assertFalse(modifiedFamily1);

    // it's where WAL write cp should occur.
    long now = EnvironmentEdgeManager.currentTime();
    // we use HLogKey here instead of WALKeyImpl directly to support legacy coprocessors.
    long txid = log.appendData(hri, new WALKeyImpl(hri.getEncodedNameAsBytes(), hri.getTable(), now,
      new MultiVersionConcurrencyControl(), scopes), edit);
    log.sync(txid);

    // the edit shall have been change now by the coprocessor.
    foundFamily0 = false;
    foundFamily2 = false;
    modifiedFamily1 = false;
    for (Cell cell : cells) {
      if (Arrays.equals(CellUtil.cloneFamily(cell), TEST_FAMILY[0])) {
        foundFamily0 = true;
      }
      if (Arrays.equals(CellUtil.cloneFamily(cell), TEST_FAMILY[2])) {
        foundFamily2 = true;
      }
      if (Arrays.equals(CellUtil.cloneFamily(cell), TEST_FAMILY[1])) {
        if (!Arrays.equals(CellUtil.cloneValue(cell), TEST_VALUE[1])) {
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
   * Coprocessors shouldn't get notice of empty waledits.
   */
  @Test
  public void testEmptyWALEditAreNotSeen() throws Exception {
    RegionInfo hri = createBasicHRegionInfo(Bytes.toString(TEST_TABLE));
    TableDescriptor htd = createBasic3FamilyHTD(Bytes.toString(TEST_TABLE));
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    WAL log = wals.getWAL(null);
    try {
      SampleRegionWALCoprocessor cp = getCoprocessor(log, SampleRegionWALCoprocessor.class);

      cp.setTestValues(TEST_TABLE, null, null, null, null, null, null, null);

      assertFalse(cp.isPreWALWriteCalled());
      assertFalse(cp.isPostWALWriteCalled());

      final long now = EnvironmentEdgeManager.currentTime();
      long txid = log.appendData(hri,
        new WALKeyImpl(hri.getEncodedNameAsBytes(), hri.getTable(), now, mvcc, scopes),
        new WALEdit());
      log.sync(txid);

      assertFalse("Empty WALEdit should skip coprocessor evaluation.", cp.isPreWALWriteCalled());
      assertFalse("Empty WALEdit should skip coprocessor evaluation.", cp.isPostWALWriteCalled());
    } finally {
      log.close();
    }
  }

  /**
   * Test WAL replay behavior with WALObserver.
   */
  @Test
  public void testWALCoprocessorReplay() throws Exception {
    // WAL replay is handled at HRegion::replayRecoveredEdits(), which is
    // ultimately called by HRegion::initialize()
    TableName tableName = TableName.valueOf(currentTest.getMethodName());
    TableDescriptor htd = getBasic3FamilyHTableDescriptor(tableName);
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    // final HRegionInfo hri =
    // createBasic3FamilyHRegionInfo(Bytes.toString(tableName));
    // final HRegionInfo hri1 =
    // createBasic3FamilyHRegionInfo(Bytes.toString(tableName));
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).build();

    final Path basedir = CommonFSUtils.getTableDir(this.hbaseRootDir, tableName);
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));

    final Configuration newConf = HBaseConfiguration.create(this.conf);

    // WAL wal = new WAL(this.fs, this.dir, this.oldLogDir, this.conf);
    WAL wal = wals.getWAL(null);
    // Put p = creatPutWith2Families(TEST_ROW);
    WALEdit edit = new WALEdit();
    long now = EnvironmentEdgeManager.currentTime();
    final int countPerFamily = 1000;
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    for (byte[] fam : htd.getColumnFamilyNames()) {
      addWALEdits(tableName, hri, TEST_ROW, fam, countPerFamily,
        EnvironmentEdgeManager.getDelegate(), wal, scopes, mvcc);
    }
    wal.appendData(hri, new WALKeyImpl(hri.getEncodedNameAsBytes(), tableName, now, mvcc, scopes),
      edit);
    // sync to fs.
    wal.sync();

    User user = HBaseTestingUtility.getDifferentUser(newConf,
        ".replay.wal.secondtime");
    user.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Path p = runWALSplit(newConf);
        LOG.info("WALSplit path == " + p);
        // Make a new wal for new region open.
        final WALFactory wals2 = new WALFactory(conf,
            ServerName.valueOf(currentTest.getMethodName() + "2", 16010, System.currentTimeMillis())
                .toString());
        WAL wal2 = wals2.getWAL(null);
        HRegion region = HRegion.openHRegion(newConf, FileSystem.get(newConf), hbaseRootDir,
            hri, htd, wal2, TEST_UTIL.getHBaseCluster().getRegionServer(0), null);

        SampleRegionWALCoprocessor cp2 =
          region.getCoprocessorHost().findCoprocessor(SampleRegionWALCoprocessor.class);
        // TODO: asserting here is problematic.
        assertNotNull(cp2);
        assertTrue(cp2.isPreWALRestoreCalled());
        assertTrue(cp2.isPostWALRestoreCalled());
        region.close();
        wals2.close();
        return null;
      }
    });
  }

  /**
   * Test to see CP loaded successfully or not. There is a duplication at
   * TestHLog, but the purpose of that one is to see whether the loaded CP will
   * impact existing WAL tests or not.
   */
  @Test
  public void testWALObserverLoaded() throws Exception {
    WAL log = wals.getWAL(null);
    assertNotNull(getCoprocessor(log, SampleRegionWALCoprocessor.class));
  }

  @Test
  public void testWALObserverRoll() throws Exception {
    final WAL wal = wals.getWAL(null);
    final SampleRegionWALCoprocessor cp = getCoprocessor(wal, SampleRegionWALCoprocessor.class);
    cp.setTestValues(TEST_TABLE, null, null, null, null, null, null, null);

    assertFalse(cp.isPreWALRollCalled());
    assertFalse(cp.isPostWALRollCalled());

    wal.rollWriter(true);
    assertTrue(cp.isPreWALRollCalled());
    assertTrue(cp.isPostWALRollCalled());
  }

  private SampleRegionWALCoprocessor getCoprocessor(WAL wal,
      Class<? extends SampleRegionWALCoprocessor> clazz) throws Exception {
    WALCoprocessorHost host = wal.getCoprocessorHost();
    Coprocessor c = host.findCoprocessor(clazz.getName());
    return (SampleRegionWALCoprocessor) c;
  }

  /**
   * Creates an HRI around an HTD that has <code>tableName</code>.
   * @param tableName Name of table to use.
   */
  private RegionInfo createBasicHRegionInfo(String tableName) {
    return RegionInfoBuilder.newBuilder(TableName.valueOf(tableName)).build();
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
      p.addColumn(TEST_FAMILY[i], TEST_QUALIFIER[i], TEST_VALUE[i]);
    }
    return p;
  }

  private Path runWALSplit(final Configuration c) throws IOException {
    List<Path> splits = WALSplitter.split(
      hbaseRootDir, logDir, oldLogDir, FileSystem.get(c), c, wals);
    // Split should generate only 1 file since there's only 1 region
    assertEquals(1, splits.size());
    // Make sure the file exists
    assertTrue(fs.exists(splits.get(0)));
    LOG.info("Split file=" + splits.get(0));
    return splits.get(0);
  }

  private void addWALEdits(final TableName tableName, final RegionInfo hri, final byte[] rowName,
      final byte[] family, final int count, EnvironmentEdge ee, final WAL wal,
      final NavigableMap<byte[], Integer> scopes, final MultiVersionConcurrencyControl mvcc)
      throws IOException {
    String familyStr = Bytes.toString(family);
    long txid = -1;
    for (int j = 0; j < count; j++) {
      byte[] qualifierBytes = Bytes.toBytes(Integer.toString(j));
      byte[] columnBytes = Bytes.toBytes(familyStr + ":" + Integer.toString(j));
      WALEdit edit = new WALEdit();
      edit.add(new KeyValue(rowName, family, qualifierBytes, ee.currentTime(), columnBytes));
      // uses WALKeyImpl instead of HLogKey on purpose. will only work for tests where we don't care
      // about legacy coprocessors
      txid = wal.appendData(hri,
        new WALKeyImpl(hri.getEncodedNameAsBytes(), tableName, ee.currentTime(), mvcc), edit);
    }
    if (-1 != txid) {
      wal.sync(txid);
    }
  }

  private TableDescriptor getBasic3FamilyHTableDescriptor(TableName tableName) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    Arrays.stream(TEST_FAMILY).map(ColumnFamilyDescriptorBuilder::of)
        .forEachOrdered(builder::setColumnFamily);
    return builder.build();
  }

  private TableDescriptor createBasic3FamilyHTD(String tableName) {
    return TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("a"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("b"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("c")).build();
  }
}

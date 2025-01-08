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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.hbase.wal.WALEditInternalHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.UUID;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALKey;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationSink {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationSink.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationSink.class);
  private static final int BATCH_SIZE = 10;

  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  protected static ReplicationSink SINK;

  protected static final TableName TABLE_NAME1 = TableName.valueOf("table1");
  protected static final TableName TABLE_NAME2 = TableName.valueOf("table2");
  protected static final TableName TABLE_NAME3 = TableName.valueOf("table3");
  protected static final TableName TABLE_NAME4 = TableName.valueOf("table4");
  protected static final TableName TABLE_NAME5 = TableName.valueOf("table5");
  protected static final TableName TABLE_NAME6 = TableName.valueOf("table6");

  protected static final byte[] FAM_NAME1 = Bytes.toBytes("info1");
  protected static final byte[] FAM_NAME2 = Bytes.toBytes("info2");

  protected static final TestReplicationSinkTranslator TRANSLATOR = new TestReplicationSinkTranslator();

  protected static Stoppable STOPPABLE = new Stoppable() {
    final AtomicBoolean stop = new AtomicBoolean(false);

    @Override
    public boolean isStopped() {
      return this.stop.get();
    }

    @Override
    public void stop(String why) {
      LOG.info("STOPPING BECAUSE: " + why);
      this.stop.set(true);
    }

  };

  protected static Table table1;
  protected static Table table2;
  protected static Table table3;
  protected static Table table4;
  protected static Table table5;
  protected static Table table6;
  protected static String baseNamespaceDir;
  protected static String hfileArchiveDir;
  protected static String replicationClusterId;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.replication.source.fs.conf.provider",
      TestSourceFSConfigurationProvider.class.getCanonicalName());
    TEST_UTIL.getConfiguration().setClass(HConstants.REPLICATION_SINK_TRANSLATOR,
      TestReplicationSinkTranslator.class, ReplicationSinkTranslator.class);
    TEST_UTIL.startMiniCluster(3);
    RegionServerCoprocessorHost rsCpHost =
      TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getRegionServerCoprocessorHost();
    SINK = new ReplicationSink(new Configuration(TEST_UTIL.getConfiguration()), rsCpHost);
    table1 = TEST_UTIL.createTable(TABLE_NAME1, FAM_NAME1);
    table2 = TEST_UTIL.createTable(TABLE_NAME2, FAM_NAME2);
    table3 = TEST_UTIL.createTable(TABLE_NAME3, FAM_NAME1);
    table4 = TEST_UTIL.createTable(TABLE_NAME4, FAM_NAME2);
    table5 = TEST_UTIL.createTable(TABLE_NAME5, FAM_NAME1);
    table6 = TEST_UTIL.createTable(TABLE_NAME6, FAM_NAME1);
    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    baseNamespaceDir = new Path(rootDir, new Path(HConstants.BASE_NAMESPACE_DIR)).toString();
    hfileArchiveDir = new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY)).toString();
    replicationClusterId = "12345";
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    STOPPABLE.stop("Shutting down");
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    table1 = TEST_UTIL.deleteTableData(TABLE_NAME1);
    table2 = TEST_UTIL.deleteTableData(TABLE_NAME2);
    table3 = TEST_UTIL.deleteTableData(TABLE_NAME3);
    table4 = TEST_UTIL.deleteTableData(TABLE_NAME4);
    table5 = TEST_UTIL.deleteTableData(TABLE_NAME5);
    table6 = TEST_UTIL.deleteTableData(TABLE_NAME6);
  }

  /**
   * Insert a whole batch of entries
   */
  public void testBatchSink(TableName tableName, byte[] family, Table sinkTable) throws Exception {
    List<WALEntry> entries = new ArrayList<>(BATCH_SIZE);
    List<ExtendedCell> cells = new ArrayList<>();
    Set<Integer> rows = new HashSet<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
      rows.add(i);
    }
    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Scan scan = new Scan();
    ResultScanner scanRes = sinkTable.getScanner(scan);
    assertEquals(BATCH_SIZE, scanRes.next(BATCH_SIZE).length);
    validatePuts(tableName, rows, family, family, sinkTable);
  }

  @Test
  public void testBatchSink() throws Exception {
    testBatchSink(TABLE_NAME1, FAM_NAME1, table1);
  }

  @Test
  public void testBatchSinkWithTranslation() throws Exception {
    testBatchSink(TABLE_NAME3, FAM_NAME1, table4);
  }

  /**
   * Insert a mix of puts and deletes
   */
  public void testMixedPutDelete(TableName tableName, byte[] family, Table sinkTable) throws Exception {
    List<WALEntry> entries = new ArrayList<>(BATCH_SIZE / 2);
    List<ExtendedCell> cells = new ArrayList<>();
    Set<Integer> rows = new HashSet<>();
    for (int i = 0; i < BATCH_SIZE / 2; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
      rows.add(i);
    }
    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);

    entries = new ArrayList<>(BATCH_SIZE);
    cells = new ArrayList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] row = Bytes.toBytes(i);
      if (i % 2 != 0) {
        entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
        rows.add(i);
      } else {
        entries.add(createEntry(tableName, row, family, family, null, KeyValue.Type.DeleteColumn, cells));
        rows.remove(i);
      }
    }

    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Scan scan = new Scan();
    ResultScanner scanRes = sinkTable.getScanner(scan);
    assertEquals(BATCH_SIZE / 2, scanRes.next(BATCH_SIZE).length);
    validatePuts(tableName, rows, family, family, sinkTable);
  }

  @Test
  public void testMixedPutDelete() throws Exception {
    testMixedPutDelete(TABLE_NAME1, FAM_NAME1, table1);
  }

  @Test
  public void testMixedPutDeleteWithTranslation() throws Exception {
    testMixedPutDelete(TABLE_NAME3, FAM_NAME1, table4);
  }

  public void testLargeEditsPutDelete(TableName tableName, byte[] family, Table sinkTable) throws Exception {
    List<WALEntry> entries = new ArrayList<>();
    List<ExtendedCell> cells = new ArrayList<>();
    Set<Integer> rows = new HashSet<>();
    for (int i = 0; i < 5510; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
      rows.add(i);
    }
    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);

    ResultScanner resultScanner = sinkTable.getScanner(new Scan());
    int totalRows = 0;
    while (resultScanner.next() != null) {
      totalRows++;
    }
    assertEquals(5510, totalRows);

    entries = new ArrayList<>();
    cells = new ArrayList<>();
    for (int i = 0; i < 11000; i++) {
      byte[] row = Bytes.toBytes(i);
      if (i % 2 != 0) {
        entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
        rows.add(i);
      } else {
        entries.add(createEntry(tableName, row, family, family, null, KeyValue.Type.DeleteColumn, cells));
        rows.remove(i);
      }
    }
    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    resultScanner = sinkTable.getScanner(new Scan());
    totalRows = 0;
    while (resultScanner.next() != null) {
      totalRows++;
    }
    assertEquals(5500, totalRows);
    validatePuts(tableName, rows, family, family, sinkTable);
  }

  @Test
  public void testLargeEditsPutDelete() throws Exception {
    testLargeEditsPutDelete(TABLE_NAME1, FAM_NAME1, table1);
  }

  @Test
  public void testLargeEditsPutDeleteWithTranslation() throws Exception {
    testLargeEditsPutDelete(TABLE_NAME3, FAM_NAME1, table4);
  }

  /**
   * Insert to 2 different tables
   */
  public void testMixedPutTables(TableName tableName1, byte[] family1, Table sinkTable1, TableName tableName2, byte[] family2,
    Table sinkTable2) throws Exception {
    List<WALEntry> entries = new ArrayList<>(BATCH_SIZE / 2);
    List<ExtendedCell> cells = new ArrayList<>();
    Set<Integer> rows1 = new HashSet<>();
    Set<Integer> rows2 = new HashSet<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] row = Bytes.toBytes(i);
      if (i % 2 == 0) {
        entries.add(createEntry(tableName2, row, family2, family2, row, KeyValue.Type.Put, cells));
        rows2.add(i);
      } else {
        entries.add(createEntry(tableName1, row, family1, family1, row, KeyValue.Type.Put, cells));
        rows1.add(i);
      }
    }

    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Scan scan = new Scan();
    ResultScanner scanRes = sinkTable2.getScanner(scan);
    for (Result res : scanRes) {
      assertEquals(0, Bytes.toInt(res.getRow()) % 2);
    }
    scanRes = sinkTable1.getScanner(scan);
    for (Result res : scanRes) {
      assertEquals(1, Bytes.toInt(res.getRow()) % 2);
    }
  }

  @Test
  public void testMixedPutTables() throws Exception {
    testMixedPutTables(TABLE_NAME1, FAM_NAME1, table1, TABLE_NAME2, FAM_NAME2, table2);
  }

  @Test
  public void testMixedPutTablesWithTranslation() throws Exception {
    testMixedPutTables(TABLE_NAME3, FAM_NAME1, table4, TABLE_NAME2, FAM_NAME2, table2);
  }

  /**
   * Insert then do different types of deletes
   */
  public void testMixedDeletes(TableName tableName, byte[] family, Table sinkTable) throws Exception {
    List<WALEntry> entries = new ArrayList<>(3);
    List<ExtendedCell> cells = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    entries = new ArrayList<>(3);
    cells = new ArrayList<>();
    entries.add(createEntry(tableName, Bytes.toBytes(0), family, family, null, KeyValue.Type.DeleteColumn, cells));
    entries.add(createEntry(tableName, Bytes.toBytes(1), family, null, null, KeyValue.Type.DeleteFamily, cells));
    entries.add(createEntry(tableName, Bytes.toBytes(2), family, family, null, KeyValue.Type.DeleteColumn, cells));

    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);

    Scan scan = new Scan();
    ResultScanner scanRes = sinkTable.getScanner(scan);
    assertEquals(0, scanRes.next(3).length);
  }

  @Test
  public void testMixedDeletes() throws Exception {
    testMixedDeletes(TABLE_NAME1, FAM_NAME1, table1);
  }

  @Test
  public void testMixedDeletesWithTranslation() throws Exception {
    testMixedDeletes(TABLE_NAME3, FAM_NAME1, table4);
  }

  /**
   * Puts are buffered, but this tests when a delete (not-buffered) is applied before the actual Put
   * that creates it.
   */
  public void testApplyDeleteBeforePut(TableName tableName, byte[] family, Table sinkTable) throws Exception {
    List<WALEntry> entries = new ArrayList<>(5);
    List<ExtendedCell> cells = new ArrayList<>();
    Set<Integer> rows = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
      rows.add(i);
    }
    int deleteRow = 1;
    entries.add(createEntry(tableName, Bytes.toBytes(1), family, null, null, KeyValue.Type.DeleteFamily, cells));
    rows.remove(deleteRow);
    for (int i = 3; i < 5; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
      rows.add(i);
    }
    SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Get get = new Get(TRANSLATOR.getSinkRowKey(tableName, Bytes.toBytes(deleteRow)));
    Result res = sinkTable.get(get);
    assertEquals(0, res.size());
    validatePuts(tableName, rows, family, family, sinkTable);
  }

  @Test
  public void testApplyDeleteBeforePut() throws Exception {
    testApplyDeleteBeforePut(TABLE_NAME1, FAM_NAME1, table1);
  }

  @Test
  public void testApplyDeleteBeforePutWithTranslation() throws Exception {
    testApplyDeleteBeforePut(TABLE_NAME3, FAM_NAME1, table4);
  }

  public void testRethrowRetriesExhaustedException(TableName tableName, byte[] family, TableName sinkTableName)
    throws Exception {
    TableName notExistTable = TableName.valueOf("notExistTable");
    byte[] notExistFamily = Bytes.toBytes("notExistFamily");
    List<WALEntry> entries = new ArrayList<>();
    List<ExtendedCell> cells = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(notExistTable, row, notExistFamily, notExistFamily, row, KeyValue.Type.Put, cells));
    }
    try {
      SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
        replicationClusterId, baseNamespaceDir, hfileArchiveDir);
      Assert.fail("Should re-throw TableNotFoundException.");
    } catch (TableNotFoundException e) {
    }
    entries.clear();
    cells.clear();
    for (int i = 0; i < 10; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
    }
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      try (Admin admin = conn.getAdmin()) {
        admin.disableTable(sinkTableName);
        try {
          SINK.replicateEntries(entries,
            PrivateCellUtil.createExtendedCellScanner(cells.iterator()), replicationClusterId,
            baseNamespaceDir, hfileArchiveDir);
          Assert.fail("Should re-throw RetriesExhaustedWithDetailsException.");
        } catch (RetriesExhaustedException e) {
        } finally {
          admin.enableTable(sinkTableName);
        }
      }
    }
  }

  @Test
  public void testRethrowRetriesExhaustedException() throws Exception {
    testRethrowRetriesExhaustedException(TABLE_NAME1, FAM_NAME1, TABLE_NAME1);
  }

  @Test
  public void testRethrowRetriesExhaustedExceptionWithTranslation() throws Exception {
    testRethrowRetriesExhaustedException(TABLE_NAME3, FAM_NAME1, TABLE_NAME4);
  }

  /**
   * Test replicateEntries with a bulk load entry for 25 HFiles
   */
  public void testReplicateEntriesForHFiles(TableName tableName, byte[] family, Table sinkTable)
    throws Exception {
    Path dir = TEST_UTIL.getDataTestDirOnTestFS("testReplicateEntries");
    Path familyDir = new Path(dir, Bytes.toString(FAM_NAME1));
    int numRows = 10;
    List<Path> p = new ArrayList<>(1);
    final String hfilePrefix = "hfile-";

    // 1. Generate 25 hfile ranges
    Random rand = ThreadLocalRandom.current();
    Set<Integer> numbers = new HashSet<>();
    while (numbers.size() < 50) {
      numbers.add(rand.nextInt(1000));
    }
    List<Integer> numberList = new ArrayList<>(numbers);
    Collections.sort(numberList);
    Map<String, Long> storeFilesSize = new HashMap<>(1);

    // 2. Create 25 hfiles
    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = dir.getFileSystem(conf);
    Iterator<Integer> numbersItr = numberList.iterator();
    for (int i = 0; i < 25; i++) {
      Path hfilePath = new Path(familyDir, hfilePrefix + i);
      HFileTestUtil.createHFile(conf, fs, hfilePath, family, family,
        Bytes.toBytes(numbersItr.next()), Bytes.toBytes(numbersItr.next()), numRows);
      p.add(hfilePath);
      storeFilesSize.put(hfilePath.getName(), fs.getFileStatus(hfilePath).getLen());
    }

    // 3. Create a BulkLoadDescriptor and a WALEdit
    Map<byte[], List<Path>> storeFiles = new HashMap<>(1);
    storeFiles.put(family, p);
    org.apache.hadoop.hbase.wal.WALEdit edit = null;
    WALProtos.BulkLoadDescriptor loadDescriptor = null;

    try (Connection c = ConnectionFactory.createConnection(conf);
      RegionLocator l = c.getRegionLocator(tableName)) {
      RegionInfo regionInfo = l.getAllRegionLocations().get(0).getRegion();
      loadDescriptor = ProtobufUtil.toBulkLoadDescriptor(tableName,
        UnsafeByteOperations.unsafeWrap(regionInfo.getEncodedNameAsBytes()), storeFiles,
        storeFilesSize, 1);
      edit = org.apache.hadoop.hbase.wal.WALEdit.createBulkLoadEvent(regionInfo, loadDescriptor);
    }
    List<WALEntry> entries = new ArrayList<>(1);

    // 4. Create a WALEntryBuilder
    WALEntry.Builder builder = createWALEntryBuilder(tableName);

    // 5. Copy the hfile to the path as it is in reality
    for (int i = 0; i < 25; i++) {
      String pathToHfileFromNS = new StringBuilder(100).append(tableName.getNamespaceAsString())
        .append(Path.SEPARATOR).append(Bytes.toString(tableName.getName())).append(Path.SEPARATOR)
        .append(Bytes.toString(loadDescriptor.getEncodedRegionName().toByteArray()))
        .append(Path.SEPARATOR).append(Bytes.toString(FAM_NAME1)).append(Path.SEPARATOR)
        .append(hfilePrefix + i).toString();
      String dst = baseNamespaceDir + Path.SEPARATOR + pathToHfileFromNS;
      Path dstPath = new Path(dst);
      FileUtil.copy(fs, p.get(0), fs, dstPath, false, conf);
    }

    entries.add(builder.build());
    try (ResultScanner scanner = sinkTable.getScanner(new Scan())) {
      // 6. Assert no existing data in table
      assertEquals(0, scanner.next(numRows).length);
    }
    // 7. Replicate the bulk loaded entry
    SINK.replicateEntries(entries,
      PrivateCellUtil
        .createExtendedCellScanner(WALEditInternalHelper.getExtendedCells(edit).iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    try (ResultScanner scanner = sinkTable.getScanner(new Scan())) {
      // 8. Assert data is replicated
      assertEquals(numRows, scanner.next(numRows).length);
    }
    // Clean up the created hfiles, or it will mess up subsequent tests
  }

  @Test
  public void testReplicateEntriesForHFiles() throws Exception {
    testReplicateEntriesForHFiles(TABLE_NAME1, FAM_NAME1, table1);
  }

  @Test
  public void testReplicateEntriesForHFilesWithTranslation() throws Exception {
    testReplicateEntriesForHFiles(TABLE_NAME5, FAM_NAME1, table6);
  }

  /**
   * Test failure metrics produced for failed replication edits
   */
  public void testFailedReplicationSinkMetrics(TableName tableName, byte[] family, TableName sinkTableName)
    throws IOException {
    long initialFailedBatches = SINK.getSinkMetrics().getFailedBatches();
    long errorCount = 0L;
    List<WALEntry> entries = new ArrayList<>(BATCH_SIZE);
    List<ExtendedCell> cells = new ArrayList<>();
    Set<Integer> rows = new HashSet<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
      rows.add(i);
    }
    cells.clear(); // cause IndexOutOfBoundsException
    try {
      SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
        replicationClusterId, baseNamespaceDir, hfileArchiveDir);
      Assert.fail("Should re-throw ArrayIndexOutOfBoundsException.");
    } catch (ArrayIndexOutOfBoundsException e) {
      errorCount++;
      assertEquals(initialFailedBatches + errorCount, SINK.getSinkMetrics().getFailedBatches());
    }

    entries.clear();
    cells.clear();
    TableName notExistTable = TableName.valueOf("notExistTable"); // cause TableNotFoundException
    byte[] notExistFamily = Bytes.toBytes("notExistFamily");
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(notExistTable, row, notExistFamily, notExistFamily, row, KeyValue.Type.Put, cells));
    }
    try {
      SINK.replicateEntries(entries, PrivateCellUtil.createExtendedCellScanner(cells.iterator()),
        replicationClusterId, baseNamespaceDir, hfileArchiveDir);
      Assert.fail("Should re-throw TableNotFoundException.");
    } catch (TableNotFoundException e) {
      errorCount++;
      assertEquals(initialFailedBatches + errorCount, SINK.getSinkMetrics().getFailedBatches());
    }

    entries.clear();
    cells.clear();
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] row = Bytes.toBytes(i);
      entries.add(createEntry(tableName, row, family, family, row, KeyValue.Type.Put, cells));
    }
    // cause IOException in batch()
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      try (Admin admin = conn.getAdmin()) {
        admin.disableTable(sinkTableName);
        try {
          SINK.replicateEntries(entries,
            PrivateCellUtil.createExtendedCellScanner(cells.iterator()), replicationClusterId,
            baseNamespaceDir, hfileArchiveDir);
          Assert.fail("Should re-throw IOException.");
        } catch (IOException e) {
          errorCount++;
          assertEquals(initialFailedBatches + errorCount, SINK.getSinkMetrics().getFailedBatches());
        } finally {
          admin.enableTable(sinkTableName);
        }
      }
    }
  }

  @Test
  public void testFailedReplicationSinkMetrics() throws IOException {
    testFailedReplicationSinkMetrics(TABLE_NAME1, FAM_NAME1, TABLE_NAME1);
  }

  @Test
  public void testFailedReplicationSinkMetricsWithTranslation() throws IOException {
    testFailedReplicationSinkMetrics(TABLE_NAME3, FAM_NAME1, TABLE_NAME4);
  }

  private void validatePuts(TableName tableName, Set<Integer> rows, byte[] family, byte[] qualifier,
    Table sinkTable)
    throws IOException {
    for (int row : rows) {
      byte[] rowBytes = Bytes.toBytes(row);
      Get get = new Get(TRANSLATOR.getSinkRowKey(tableName, rowBytes));
      Result res = sinkTable.get(get);
      assertTrue(!res.isEmpty());
      assertArrayEquals(res.getValue(TRANSLATOR.getSinkFamily(tableName, family), TRANSLATOR.getSinkQualifier(tableName, family, qualifier)), rowBytes);
    }
  }

  private WALEntry createEntry(TableName table, byte[] row, byte[] family, byte[] qualifier,
    byte[] value, KeyValue.Type type, List<ExtendedCell> cells) {
    // Just make sure we don't get the same ts for two consecutive rows with
    // same key
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      LOG.info("Was interrupted while sleep, meh", e);
    }
    final long now = EnvironmentEdgeManager.currentTime();
    KeyValue kv = null;
    if (type.getCode() == KeyValue.Type.Put.getCode()) {
      kv = new KeyValue(row, family, qualifier, now, KeyValue.Type.Put, value);
    } else if (type.getCode() == KeyValue.Type.DeleteColumn.getCode()) {
      kv = new KeyValue(row, family, qualifier, now, KeyValue.Type.DeleteColumn);
    } else if (type.getCode() == KeyValue.Type.DeleteFamily.getCode()) {
      kv = new KeyValue(row, family, null, now, KeyValue.Type.DeleteFamily);
    }
    WALEntry.Builder builder = createWALEntryBuilder(table);
    cells.add(kv);

    return builder.build();
  }

  public static WALEntry.Builder createWALEntryBuilder(TableName tableName) {
    WALEntry.Builder builder = WALEntry.newBuilder();
    builder.setAssociatedCellCount(1);
    WALKey.Builder keyBuilder = WALKey.newBuilder();
    UUID.Builder uuidBuilder = UUID.newBuilder();
    uuidBuilder.setLeastSigBits(HConstants.DEFAULT_CLUSTER_ID.getLeastSignificantBits());
    uuidBuilder.setMostSigBits(HConstants.DEFAULT_CLUSTER_ID.getMostSignificantBits());
    keyBuilder.setClusterId(uuidBuilder.build());
    keyBuilder.setTableName(UnsafeByteOperations.unsafeWrap(tableName.getName()));
    keyBuilder.setWriteTime(EnvironmentEdgeManager.currentTime());
    keyBuilder.setEncodedRegionName(UnsafeByteOperations.unsafeWrap(HConstants.EMPTY_BYTE_ARRAY));
    keyBuilder.setLogSequenceNumber(-1);
    builder.setKey(keyBuilder.build());
    return builder;
  }

  public static class TestReplicationSinkTranslator implements ReplicationSinkTranslator {

    private static final ExtendedCellBuilder EXTENDED_CELL_BUILDER =
      ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY);

    @Override
    public TableName getSinkTableName(TableName tableName) {
      if (tableName.equals(TABLE_NAME3)) {
        return TABLE_NAME4;
      } else if (tableName.equals(TABLE_NAME5)) {
        return TABLE_NAME6;
      }
      return tableName;
    }

    @Override
    public byte[] getSinkRowKey(TableName tableName, byte[] rowKey) {
      return rowKey;
    }

    @Override
    public byte[] getSinkFamily(TableName tableName, byte[] family) {
      if (tableName.equals(TABLE_NAME3) && Bytes.equals(family, FAM_NAME1)) {
        return FAM_NAME2;
      }
      return family;
    }

    @Override
    public byte[] getSinkQualifier(TableName tableName, byte[] family, byte[] qualifier) {
      return qualifier;
    }

    @Override
    public ExtendedCell getSinkExtendedCell(TableName tableName, ExtendedCell cell) {
      if (
        tableName.equals(TABLE_NAME3) && Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(),
          cell.getFamilyLength(), FAM_NAME1, 0, FAM_NAME1.length)
      ) {
        return EXTENDED_CELL_BUILDER.clear()
          .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()).setFamily(FAM_NAME2)
          .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength())
          .setTimestamp(cell.getTimestamp())
          .setTags(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength())
          .setSequenceId(cell.getSequenceId()).setType(cell.getType())
          .setValue(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()).build();
      }
      return cell;
    }
  }
}

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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.UUID;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationSink {
  private static final Log LOG = LogFactory.getLog(TestReplicationSink.class);
  private static final int BATCH_SIZE = 10;

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static ReplicationSink SINK;

  protected static final TableName TABLE_NAME1 = TableName.valueOf("table1");
  protected static final TableName TABLE_NAME2 = TableName.valueOf("table2");

  protected static final byte[] FAM_NAME1 = Bytes.toBytes("info1");
  protected static final byte[] FAM_NAME2 = Bytes.toBytes("info2");

  protected static Table table1;
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

  protected static Table table2;
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

    TEST_UTIL.startMiniCluster(3);
    SINK =
      new ReplicationSink(new Configuration(TEST_UTIL.getConfiguration()), STOPPABLE);
    table1 = TEST_UTIL.createTable(TABLE_NAME1, FAM_NAME1);
    table2 = TEST_UTIL.createTable(TABLE_NAME2, FAM_NAME2);
    Path rootDir = FSUtils.getRootDir(TEST_UTIL.getConfiguration());
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
  }

  /**
   * Insert a whole batch of entries
   * @throws Exception
   */
  @Test
  public void testBatchSink() throws Exception {
    List<WALEntry> entries = new ArrayList<WALEntry>(BATCH_SIZE);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert a mix of puts and deletes
   * @throws Exception
   */
  @Test
  public void testMixedPutDelete() throws Exception {
    List<WALEntry> entries = new ArrayList<WALEntry>(BATCH_SIZE/2);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE/2; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells), replicationClusterId,
      baseNamespaceDir, hfileArchiveDir);

    entries = new ArrayList<WALEntry>(BATCH_SIZE);
    cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries.add(createEntry(TABLE_NAME1, i,
          i % 2 != 0 ? KeyValue.Type.Put: KeyValue.Type.DeleteColumn, cells));
    }

    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE/2, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert to 2 different tables
   * @throws Exception
   */
  @Test
  public void testMixedPutTables() throws Exception {
    List<WALEntry> entries = new ArrayList<WALEntry>(BATCH_SIZE/2);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries.add(createEntry( i % 2 == 0 ? TABLE_NAME2 : TABLE_NAME1,
              i, KeyValue.Type.Put, cells));
    }

    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Scan scan = new Scan();
    ResultScanner scanRes = table2.getScanner(scan);
    for(Result res : scanRes) {
      assertTrue(Bytes.toInt(res.getRow()) % 2 == 0);
    }
  }

  /**
   * Insert then do different types of deletes
   * @throws Exception
   */
  @Test
  public void testMixedDeletes() throws Exception {
    List<WALEntry> entries = new ArrayList<WALEntry>(3);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < 3; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    entries = new ArrayList<WALEntry>(3);
    cells = new ArrayList<Cell>();
    entries.add(createEntry(TABLE_NAME1, 0, KeyValue.Type.DeleteColumn, cells));
    entries.add(createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily, cells));
    entries.add(createEntry(TABLE_NAME1, 2, KeyValue.Type.DeleteColumn, cells));

    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);

    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(0, scanRes.next(3).length);
  }

  /**
   * Puts are buffered, but this tests when a delete (not-buffered) is applied
   * before the actual Put that creates it.
   * @throws Exception
   */
  @Test
  public void testApplyDeleteBeforePut() throws Exception {
    List<WALEntry> entries = new ArrayList<WALEntry>(5);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < 2; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    entries.add(createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily, cells));
    for(int i = 3; i < 5; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()),
      replicationClusterId, baseNamespaceDir, hfileArchiveDir);
    Get get = new Get(Bytes.toBytes(1));
    Result res = table1.get(get);
    assertEquals(0, res.size());
  }

  /**
   * Test replicateEntries with a bulk load entry for 25 HFiles
   */
  @Test
  public void testReplicateEntriesForHFiles() throws Exception {
    Path dir = TEST_UTIL.getDataTestDirOnTestFS("testReplicateEntries");
    Path familyDir = new Path(dir, Bytes.toString(FAM_NAME1));
    int numRows = 10;

    List<Path> p = new ArrayList<>(1);

    // 1. Generate 25 hfile ranges
    Random rng = new SecureRandom();
    Set<Integer> numbers = new HashSet<>();
    while (numbers.size() < 50) {
      numbers.add(rng.nextInt(1000));
    }
    List<Integer> numberList = new ArrayList<>(numbers);
    Collections.sort(numberList);
    Map<String, Long> storeFilesSize = new HashMap<String, Long>(1);

    // 2. Create 25 hfiles
    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = dir.getFileSystem(conf);
    Iterator<Integer> numbersItr = numberList.iterator();
    for (int i = 0; i < 25; i++) {
      Path hfilePath = new Path(familyDir, "hfile_" + i);
      HFileTestUtil.createHFile(conf, fs, hfilePath, FAM_NAME1, FAM_NAME1,
        Bytes.toBytes(numbersItr.next()), Bytes.toBytes(numbersItr.next()), numRows);
      p.add(hfilePath);
      storeFilesSize.put(hfilePath.getName(), fs.getFileStatus(hfilePath).getLen());
    }

    // 3. Create a BulkLoadDescriptor and a WALEdit
    Map<byte[], List<Path>> storeFiles = new HashMap<>(1);
    storeFiles.put(FAM_NAME1, p);
    WALEdit edit = null;
    WALProtos.BulkLoadDescriptor loadDescriptor = null;

    try (Connection c = ConnectionFactory.createConnection(conf);
        RegionLocator l = c.getRegionLocator(TABLE_NAME1)) {
      HRegionInfo regionInfo = l.getAllRegionLocations().get(0).getRegionInfo();
      loadDescriptor =
          ProtobufUtil.toBulkLoadDescriptor(TABLE_NAME1,
              UnsafeByteOperations.unsafeWrap(regionInfo.getEncodedNameAsBytes()),
              storeFiles, storeFilesSize, 1);
      edit = WALEdit.createBulkLoadEvent(regionInfo, loadDescriptor);
    }
    List<WALEntry> entries = new ArrayList<WALEntry>(1);

    // 4. Create a WALEntryBuilder
    WALEntry.Builder builder = createWALEntryBuilder(TABLE_NAME1);

    // 5. Copy the hfile to the path as it is in reality
    for (int i = 0; i < 25; i++) {
      String pathToHfileFromNS =
          new StringBuilder(100).append(TABLE_NAME1.getNamespaceAsString()).append(Path.SEPARATOR)
              .append(Bytes.toString(TABLE_NAME1.getName())).append(Path.SEPARATOR)
              .append(Bytes.toString(loadDescriptor.getEncodedRegionName().toByteArray()))
              .append(Path.SEPARATOR).append(Bytes.toString(FAM_NAME1)).append(Path.SEPARATOR)
              .append("hfile_" + i).toString();
      String dst = baseNamespaceDir + Path.SEPARATOR + pathToHfileFromNS;

      FileUtil.copy(fs, p.get(0), fs, new Path(dst), false, conf);
    }

    entries.add(builder.build());
    ResultScanner scanRes = null;
    try {
      Scan scan = new Scan();
      scanRes = table1.getScanner(scan);
      // 6. Assert no existing data in table
      assertEquals(0, scanRes.next(numRows).length);
      // 7. Replicate the bulk loaded entry
      SINK.replicateEntries(entries, CellUtil.createCellScanner(edit.getCells().iterator()),
        replicationClusterId, baseNamespaceDir, hfileArchiveDir);
      scanRes = table1.getScanner(scan);
      // 8. Assert data is replicated
      assertEquals(numRows, scanRes.next(numRows).length);
    } finally {
      if (scanRes != null) {
        scanRes.close();
      }
    }
  }

  private WALEntry createEntry(TableName table, int row,  KeyValue.Type type, List<Cell> cells) {
    byte[] fam = table.equals(TABLE_NAME1) ? FAM_NAME1 : FAM_NAME2;
    byte[] rowBytes = Bytes.toBytes(row);
    // Just make sure we don't get the same ts for two consecutive rows with
    // same key
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      LOG.info("Was interrupted while sleep, meh", e);
    }
    final long now = System.currentTimeMillis();
    KeyValue kv = null;
    if(type.getCode() == KeyValue.Type.Put.getCode()) {
      kv = new KeyValue(rowBytes, fam, fam, now,
          KeyValue.Type.Put, Bytes.toBytes(row));
    } else if (type.getCode() == KeyValue.Type.DeleteColumn.getCode()) {
        kv = new KeyValue(rowBytes, fam, fam,
            now, KeyValue.Type.DeleteColumn);
    } else if (type.getCode() == KeyValue.Type.DeleteFamily.getCode()) {
        kv = new KeyValue(rowBytes, fam, null,
            now, KeyValue.Type.DeleteFamily);
    }
    WALEntry.Builder builder = createWALEntryBuilder(table);
    cells.add(kv);

    return builder.build();
  }

  private WALEntry.Builder createWALEntryBuilder(TableName table) {
    WALEntry.Builder builder = WALEntry.newBuilder();
    builder.setAssociatedCellCount(1);
    WALKey.Builder keyBuilder = WALKey.newBuilder();
    UUID.Builder uuidBuilder = UUID.newBuilder();
    uuidBuilder.setLeastSigBits(HConstants.DEFAULT_CLUSTER_ID.getLeastSignificantBits());
    uuidBuilder.setMostSigBits(HConstants.DEFAULT_CLUSTER_ID.getMostSignificantBits());
    keyBuilder.setClusterId(uuidBuilder.build());
    keyBuilder.setTableName(UnsafeByteOperations.unsafeWrap(table.getName()));
    keyBuilder.setWriteTime(System.currentTimeMillis());
    keyBuilder.setEncodedRegionName(UnsafeByteOperations.unsafeWrap(HConstants.EMPTY_BYTE_ARRAY));
    keyBuilder.setLogSequenceNumber(-1);
    builder.setKey(keyBuilder.build());
    return builder;
  }
}

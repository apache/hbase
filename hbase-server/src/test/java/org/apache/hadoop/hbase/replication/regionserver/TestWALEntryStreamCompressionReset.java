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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALEditInternalHelper;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;

/**
 * Enable compression and reset the WALEntryStream while reading in ReplicationSourceWALReader.
 * <p/>
 * This is used to confirm that we can work well when hitting EOFException in the middle when
 * reading a WAL entry, when compression is enabled. See HBASE-27621 for more details.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestWALEntryStreamCompressionReset {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALEntryStreamCompressionReset.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("reset");

  private static RegionInfo REGION_INFO = RegionInfoBuilder.newBuilder(TABLE_NAME).build();

  private static byte[] FAMILY = Bytes.toBytes("family");

  private static MultiVersionConcurrencyControl MVCC = new MultiVersionConcurrencyControl();

  private static NavigableMap<byte[], Integer> SCOPE;

  private static String GROUP_ID = "group";

  private static FileSystem FS;

  private static ReplicationSource SOURCE;

  private static MetricsSource METRICS_SOURCE;

  private static ReplicationSourceLogQueue LOG_QUEUE;

  private static Path TEMPLATE_WAL_FILE;

  private static int END_OFFSET_OF_WAL_ENTRIES;

  private static Path WAL_FILE;

  private static volatile long WAL_LENGTH;

  private static ReplicationSourceWALReader READER;

  // return the wal path, and also the end offset of last wal entry
  private static Pair<Path, Long> generateWAL() throws Exception {
    Path path = UTIL.getDataTestDir("wal");
    ProtobufLogWriter writer = new ProtobufLogWriter();
    writer.init(FS, path, UTIL.getConfiguration(), false, FS.getDefaultBlockSize(path), null);
    for (int i = 0; i < Byte.MAX_VALUE; i++) {
      WALEdit edit = new WALEdit();
      WALEditInternalHelper.addExtendedCell(edit,
        ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put)
          .setRow(Bytes.toBytes(i)).setFamily(FAMILY).setQualifier(Bytes.toBytes("qualifier-" + i))
          .setValue(Bytes.toBytes("v-" + i)).build());
      writer.append(new WAL.Entry(new WALKeyImpl(REGION_INFO.getEncodedNameAsBytes(), TABLE_NAME,
        EnvironmentEdgeManager.currentTime(), MVCC, SCOPE), edit));
    }

    WALEdit edit2 = new WALEdit();
    WALEditInternalHelper.addExtendedCell(edit2,
      ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put)
        .setRow(Bytes.toBytes(-1)).setFamily(FAMILY).setQualifier(Bytes.toBytes("qualifier"))
        .setValue(Bytes.toBytes("vv")).build());
    WALEditInternalHelper.addExtendedCell(edit2,
      ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put)
        .setRow(Bytes.toBytes(-1)).setFamily(FAMILY).setQualifier(Bytes.toBytes("qualifier-1"))
        .setValue(Bytes.toBytes("vvv")).build());
    writer.append(new WAL.Entry(new WALKeyImpl(REGION_INFO.getEncodedNameAsBytes(), TABLE_NAME,
      EnvironmentEdgeManager.currentTime(), MVCC, SCOPE), edit2));
    writer.sync(false);
    long offset = writer.getSyncedLength();
    writer.close();
    return Pair.newPair(path, offset);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    FS = UTIL.getTestFileSystem();
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    conf.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    conf.setInt("replication.source.maxretriesmultiplier", 1);
    FS.mkdirs(UTIL.getDataTestDir());
    Pair<Path, Long> pair = generateWAL();
    TEMPLATE_WAL_FILE = pair.getFirst();
    END_OFFSET_OF_WAL_ENTRIES = pair.getSecond().intValue();
    WAL_FILE = UTIL.getDataTestDir("rep_source");

    METRICS_SOURCE = new MetricsSource("reset");
    SOURCE = mock(ReplicationSource.class);
    when(SOURCE.isPeerEnabled()).thenReturn(true);
    when(SOURCE.getWALFileLengthProvider()).thenReturn(p -> OptionalLong.of(WAL_LENGTH));
    when(SOURCE.getServerWALsBelongTo())
      .thenReturn(ServerName.valueOf("localhost", 12345, EnvironmentEdgeManager.currentTime()));
    when(SOURCE.getSourceMetrics()).thenReturn(METRICS_SOURCE);
    ReplicationSourceManager rsm = new ReplicationSourceManager(null, null, conf, null, null, null,
      null, null, null, null, mock(MetricsReplicationGlobalSourceSource.class));
    when(SOURCE.getSourceManager()).thenReturn(rsm);

    LOG_QUEUE = new ReplicationSourceLogQueue(conf, METRICS_SOURCE, SOURCE);
    LOG_QUEUE.enqueueLog(WAL_FILE, GROUP_ID);
    READER = new ReplicationSourceWALReader(FS, conf, LOG_QUEUE, 0, e -> e, SOURCE, GROUP_ID);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    READER.setReaderRunning(false);
    READER.join();
    UTIL.cleanupTestDir();
  }

  private void test(byte[] content, FSDataOutputStream out) throws Exception {
    // minus 15 so the second entry is incomplete
    // 15 is a magic number here, we want the reader parse the first cell but not the second cell,
    // especially not the qualifier of the second cell. The value of the second cell is 'vvv', which
    // is 3 bytes, plus 8 bytes timestamp, and also qualifier, family and row(which should have been
    // compressed), so 15 is a proper value, of course 14 or 16 could also work here.
    out.write(content, 0, END_OFFSET_OF_WAL_ENTRIES - 15);
    out.hflush();
    WAL_LENGTH = END_OFFSET_OF_WAL_ENTRIES - 15;
    READER.start();
    List<WAL.Entry> entries = new ArrayList<>();
    for (;;) {
      WALEntryBatch batch = READER.poll(1000);
      if (batch == null) {
        break;
      }
      entries.addAll(batch.getWalEntries());
    }
    // should return all the entries except the last one
    assertEquals(Byte.MAX_VALUE, entries.size());
    for (int i = 0; i < Byte.MAX_VALUE; i++) {
      WAL.Entry entry = entries.get(i);
      assertEquals(1, entry.getEdit().size());
      Cell cell = entry.getEdit().getCells().get(0);
      assertEquals(i, Bytes.toInt(cell.getRowArray(), cell.getRowOffset()));
      assertEquals(Bytes.toString(FAMILY),
        Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
      assertEquals("qualifier-" + i, Bytes.toString(cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength()));
      assertEquals("v-" + i,
        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    }

    // confirm that we can not get the last one since it is incomplete
    assertNull(READER.poll(1000));
    // write the last byte out
    out.write(content, END_OFFSET_OF_WAL_ENTRIES - 15, 15);
    out.hflush();
    WAL_LENGTH = END_OFFSET_OF_WAL_ENTRIES;

    // should get the last entry
    WALEntryBatch batch = READER.poll(10000);
    assertEquals(1, batch.getNbEntries());
    WAL.Entry entry = batch.getWalEntries().get(0);
    assertEquals(2, entry.getEdit().size());
    Cell cell2 = entry.getEdit().getCells().get(0);
    assertEquals(-1, Bytes.toInt(cell2.getRowArray(), cell2.getRowOffset()));
    assertEquals(Bytes.toString(FAMILY),
      Bytes.toString(cell2.getFamilyArray(), cell2.getFamilyOffset(), cell2.getFamilyLength()));
    assertEquals("qualifier", Bytes.toString(cell2.getQualifierArray(), cell2.getQualifierOffset(),
      cell2.getQualifierLength()));
    assertEquals("vv",
      Bytes.toString(cell2.getValueArray(), cell2.getValueOffset(), cell2.getValueLength()));

    Cell cell3 = entry.getEdit().getCells().get(1);
    assertEquals(-1, Bytes.toInt(cell3.getRowArray(), cell3.getRowOffset()));
    assertEquals(Bytes.toString(FAMILY),
      Bytes.toString(cell3.getFamilyArray(), cell3.getFamilyOffset(), cell3.getFamilyLength()));
    assertEquals("qualifier-1", Bytes.toString(cell3.getQualifierArray(),
      cell3.getQualifierOffset(), cell3.getQualifierLength()));
    assertEquals("vvv",
      Bytes.toString(cell3.getValueArray(), cell3.getValueOffset(), cell3.getValueLength()));
  }

  @Test
  public void testReset() throws Exception {
    byte[] content;
    try (FSDataInputStream in = FS.open(TEMPLATE_WAL_FILE)) {
      content = ByteStreams.toByteArray(in);
    }
    try (FSDataOutputStream out = FS.create(WAL_FILE)) {
      test(content, out);
    }
  }
}

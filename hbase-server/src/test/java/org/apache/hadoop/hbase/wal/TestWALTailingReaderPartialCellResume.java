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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestWALTailingReaderPartialCellResume {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALTailingReaderPartialCellResume.class);

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private static FileSystem FS;

  private static final TableName TN = TableName.valueOf("test");
  private static final RegionInfo RI = RegionInfoBuilder.newBuilder(TN).build();
  private static final byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void setUp() throws IOException {
    UTIL.getConfiguration().setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    UTIL.getConfiguration().setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    FS = FileSystem.getLocal(UTIL.getConfiguration());
    if (!FS.mkdirs(UTIL.getDataTestDir())) {
      throw new IOException("can not create " + UTIL.getDataTestDir());
    }
  }

  @AfterClass
  public static void tearDown() {
    UTIL.cleanupTestDir();
  }

  private WAL.Entry createEntry(int index, int numCells) {
    WALKeyImpl key = new WALKeyImpl(RI.getEncodedNameAsBytes(), TN, index,
      EnvironmentEdgeManager.currentTime(), HConstants.DEFAULT_CLUSTER_ID);
    WALEdit edit = new WALEdit();
    for (int c = 0; c < numCells; c++) {
      edit.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put)
        .setRow(Bytes.toBytes("row-" + index)).setFamily(FAMILY)
        .setQualifier(Bytes.toBytes("qual-" + index + "-" + c))
        .setValue(Bytes.toBytes("value-" + index + "-" + c)).build());
    }
    return new WAL.Entry(key, edit);
  }

  @Test
  public void itReturnsEofAndResetNotCompressionOnPartialEntry() throws Exception {
    Path walFile = UTIL.getDataTestDir("wal-partial");
    List<Long> endOffsets = new ArrayList<>();
    try (WALProvider.Writer writer =
      WALFactory.createWALWriter(FS, walFile, UTIL.getConfiguration())) {
      for (int i = 0; i < 5; i++) {
        writer.append(createEntry(i, 1));
        writer.sync(true);
        endOffsets.add(writer.getLength());
      }
      writer.append(createEntry(5, 3));
      writer.sync(true);
      endOffsets.add(writer.getLength());
    }

    long fileLength = FS.getFileStatus(walFile).getLen();
    byte[] content = new byte[(int) fileLength];
    try (FSDataInputStream in = FS.open(walFile)) {
      in.readFully(content);
    }

    long lastSingleCellEnd = endOffsets.get(4);
    int truncPoint = (int) (lastSingleCellEnd + (endOffsets.get(5) - lastSingleCellEnd) / 2);
    Path truncFile = UTIL.getDataTestDir("wal-trunc");
    try (FSDataOutputStream out = FS.create(truncFile)) {
      out.write(content, 0, truncPoint);
    }

    try (WALTailingReader reader =
      WALFactory.createTailingReader(FS, truncFile, UTIL.getConfiguration(), -1)) {
      for (int i = 0; i < 5; i++) {
        WALTailingReader.Result result = reader.next(-1);
        assertEquals("State should be NORMAL for entry " + i, WALTailingReader.State.NORMAL,
          result.getState());
        assertArrayEquals("Row should match for entry " + i, Bytes.toBytes("row-" + i),
          CellUtil.cloneRow(result.getEntry().getEdit().getCells().get(0)));
      }

      WALTailingReader.Result eofResult = reader.next(-1);
      assertEquals(
        "With deferred dict updates, EOF mid-cell should return EOF_AND_RESET,"
          + " not EOF_AND_RESET_COMPRESSION",
        WALTailingReader.State.EOF_AND_RESET, eofResult.getState());
    }
  }

  @Test
  public void itResumesPartialEntryAfterReset() throws Exception {
    Path walFile = UTIL.getDataTestDir("wal-resume");
    List<Long> endOffsets = new ArrayList<>();
    try (WALProvider.Writer writer =
      WALFactory.createWALWriter(FS, walFile, UTIL.getConfiguration())) {
      for (int i = 0; i < 3; i++) {
        writer.append(createEntry(i, 1));
        writer.sync(true);
        endOffsets.add(writer.getLength());
      }
      writer.append(createEntry(3, 2));
      writer.sync(true);
      endOffsets.add(writer.getLength());
    }

    long fileLength = FS.getFileStatus(walFile).getLen();
    byte[] content = new byte[(int) fileLength];
    try (FSDataInputStream in = FS.open(walFile)) {
      in.readFully(content);
    }

    long lastSingleEnd = endOffsets.get(2);
    int truncPoint = (int) (lastSingleEnd + (endOffsets.get(3) - lastSingleEnd) / 2);
    Path truncFile = UTIL.getDataTestDir("wal-resume-trunc");
    try (FSDataOutputStream out = FS.create(truncFile)) {
      out.write(content, 0, truncPoint);
    }

    try (WALTailingReader reader =
      WALFactory.createTailingReader(FS, truncFile, UTIL.getConfiguration(), -1)) {
      for (int i = 0; i < 3; i++) {
        WALTailingReader.Result result = reader.next(-1);
        assertEquals("State should be NORMAL for entry " + i, WALTailingReader.State.NORMAL,
          result.getState());
      }

      WALTailingReader.Result eofResult = reader.next(-1);
      assertEquals(WALTailingReader.State.EOF_AND_RESET, eofResult.getState());
      long eofPos = eofResult.getEntryEndPos();

      FS.delete(truncFile, false);
      try (FSDataOutputStream out = FS.create(truncFile)) {
        out.write(content, 0, content.length);
      }

      reader.resetTo(eofPos, false);
      WALTailingReader.Result resumeResult = reader.next(-1);
      assertEquals(WALTailingReader.State.NORMAL, resumeResult.getState());
      WAL.Entry entry = resumeResult.getEntry();
      assertEquals(2, entry.getEdit().getCells().size());
      assertArrayEquals(Bytes.toBytes("row-3"),
        CellUtil.cloneRow(entry.getEdit().getCells().get(0)));
      assertArrayEquals(Bytes.toBytes("qual-3-0"),
        CellUtil.cloneQualifier(entry.getEdit().getCells().get(0)));
      assertArrayEquals(Bytes.toBytes("qual-3-1"),
        CellUtil.cloneQualifier(entry.getEdit().getCells().get(1)));
    }
  }

  @Test
  public void itHandlesMultipleConsecutivePartialReads() throws Exception {
    Path walFile = UTIL.getDataTestDir("wal-multi-partial");
    List<Long> endOffsets = new ArrayList<>();
    try (WALProvider.Writer writer =
      WALFactory.createWALWriter(FS, walFile, UTIL.getConfiguration())) {
      for (int i = 0; i < 2; i++) {
        writer.append(createEntry(i, 1));
        writer.sync(true);
        endOffsets.add(writer.getLength());
      }
      writer.append(createEntry(2, 3));
      writer.sync(true);
      endOffsets.add(writer.getLength());
    }

    long fileLength = FS.getFileStatus(walFile).getLen();
    byte[] content = new byte[(int) fileLength];
    try (FSDataInputStream in = FS.open(walFile)) {
      in.readFully(content);
    }

    long multiCellStart = endOffsets.get(1);
    long multiCellEnd = endOffsets.get(2);
    int midRange = (int) (multiCellEnd - multiCellStart);

    int truncPoint1 = (int) multiCellStart + midRange / 4;
    int truncPoint2 = (int) multiCellStart + midRange / 2;

    Path truncFile = UTIL.getDataTestDir("wal-multi-partial-trunc");
    try (FSDataOutputStream out = FS.create(truncFile)) {
      out.write(content, 0, truncPoint1);
    }

    try (WALTailingReader reader =
      WALFactory.createTailingReader(FS, truncFile, UTIL.getConfiguration(), -1)) {
      for (int i = 0; i < 2; i++) {
        WALTailingReader.Result result = reader.next(-1);
        assertEquals(WALTailingReader.State.NORMAL, result.getState());
      }

      WALTailingReader.Result eof1 = reader.next(-1);
      assertEquals(WALTailingReader.State.EOF_AND_RESET, eof1.getState());
      long pos1 = eof1.getEntryEndPos();

      FS.delete(truncFile, false);
      try (FSDataOutputStream out = FS.create(truncFile)) {
        out.write(content, 0, truncPoint2);
      }
      reader.resetTo(pos1, false);

      WALTailingReader.Result eof2 = reader.next(-1);
      assertEquals(WALTailingReader.State.EOF_AND_RESET, eof2.getState());
      long pos2 = eof2.getEntryEndPos();

      FS.delete(truncFile, false);
      try (FSDataOutputStream out = FS.create(truncFile)) {
        out.write(content, 0, content.length);
      }
      reader.resetTo(pos2, false);

      WALTailingReader.Result finalResult = reader.next(-1);
      assertEquals(WALTailingReader.State.NORMAL, finalResult.getState());
      WAL.Entry entry = finalResult.getEntry();
      assertEquals(3, entry.getEdit().getCells().size());
      assertArrayEquals(Bytes.toBytes("row-2"),
        CellUtil.cloneRow(entry.getEdit().getCells().get(0)));
    }
  }
}

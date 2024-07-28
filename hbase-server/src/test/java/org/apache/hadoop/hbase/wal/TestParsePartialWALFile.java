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
import static org.junit.Assert.assertThrows;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.wal.WALHeaderEOFException;
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

/**
 * In this test, we write a small WAL file first, and then generate partial WAL file which length is
 * in range [0, fileLength)(we test all the possible length in the range), to see if we can
 * successfully get the completed entries, and also get an EOF at the end.
 * <p/>
 * It is very important to make sure 3 things:
 * <ul>
 * <li>We do not get incorrect entries. Otherwise there will be data corruption.</li>
 * <li>We can get all the completed entries, i.e, we do not miss some data. Otherwise there will be
 * data loss.</li>
 * <li>We will get an EOF finally, instead of a general IOException. Otherwise the split or
 * replication will be stuck.</li>
 * </ul>
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestParsePartialWALFile {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestParsePartialWALFile.class);

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private static FileSystem FS;

  private static TableName TN = TableName.valueOf("test");
  private static RegionInfo RI = RegionInfoBuilder.newBuilder(TN).build();
  private static byte[] ROW = Bytes.toBytes("row");
  private static byte[] FAMILY = Bytes.toBytes("family");
  private static byte[] QUAL = Bytes.toBytes("qualifier");
  private static byte[] VALUE = Bytes.toBytes("value");

  @BeforeClass
  public static void setUp() throws IOException {
    UTIL.getConfiguration().setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    FS = FileSystem.getLocal(UTIL.getConfiguration());
    if (!FS.mkdirs(UTIL.getDataTestDir())) {
      throw new IOException("can not create " + UTIL.getDataTestDir());
    }
  }

  @AfterClass
  public static void tearDown() {
    UTIL.cleanupTestDir();
  }

  private Path generateBrokenWALFile(byte[] content, int length) throws IOException {
    Path walFile = UTIL.getDataTestDir("wal-" + length);
    try (FSDataOutputStream out = FS.create(walFile)) {
      out.write(content, 0, length);
    }
    return walFile;
  }

  private void assertEntryEquals(WAL.Entry entry, int index) {
    WALKeyImpl key = entry.getKey();
    assertEquals(TN, key.getTableName());
    assertArrayEquals(RI.getEncodedNameAsBytes(), key.getEncodedRegionName());
    WALEdit edit = entry.getEdit();
    assertEquals(1, edit.getCells().size());
    Cell cell = edit.getCells().get(0);
    assertArrayEquals(ROW, CellUtil.cloneRow(cell));
    assertArrayEquals(FAMILY, CellUtil.cloneFamily(cell));
    if (index % 2 == 0) {
      assertEquals(Type.Put, cell.getType());
      assertArrayEquals(QUAL, CellUtil.cloneQualifier(cell));
      assertArrayEquals(VALUE, CellUtil.cloneValue(cell));
    } else {
      assertEquals(Type.DeleteFamily, cell.getType());
    }
  }

  private void testReadEntry(Path file, int entryCount) throws IOException {
    try (
      WALStreamReader reader = WALFactory.createStreamReader(FS, file, UTIL.getConfiguration())) {
      for (int i = 0; i < entryCount; i++) {
        assertEntryEquals(reader.next(), i);
      }
      assertThrows(EOFException.class, () -> reader.next());
    }
    try (WALTailingReader reader =
      WALFactory.createTailingReader(FS, file, UTIL.getConfiguration(), -1)) {
      for (int i = 0; i < entryCount; i++) {
        WALTailingReader.Result result = reader.next(-1);
        assertEquals(WALTailingReader.State.NORMAL, result.getState());
        assertEntryEquals(result.getEntry(), i);
      }
      WALTailingReader.Result result = reader.next(-1);
      assertEquals(WALTailingReader.State.EOF_AND_RESET, result.getState());
    }
  }

  @Test
  public void testPartialParse() throws Exception {
    Path walFile = UTIL.getDataTestDir("wal");
    long headerLength;
    List<Long> endOffsets = new ArrayList<>();
    try (WALProvider.Writer writer =
      WALFactory.createWALWriter(FS, walFile, UTIL.getConfiguration())) {
      headerLength = writer.getLength();
      for (int i = 0; i < 3; i++) {
        WALKeyImpl key = new WALKeyImpl(RI.getEncodedNameAsBytes(), TN, i,
          EnvironmentEdgeManager.currentTime(), HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        if (i % 2 == 0) {
          edit.add(ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Type.Put)
            .setRow(ROW).setFamily(FAMILY).setQualifier(QUAL).setValue(VALUE).build());
        } else {
          edit.add(ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setType(Type.DeleteFamily).setRow(ROW).setFamily(FAMILY).build());
        }
        writer.append(new WAL.Entry(key, edit));
        writer.sync(true);
        endOffsets.add(writer.getLength());
      }
    }
    long fileLength = FS.getFileStatus(walFile).getLen();
    byte[] content = new byte[(int) fileLength];
    try (FSDataInputStream in = FS.open(walFile)) {
      in.readFully(content);
    }
    // partial header, should throw WALHeaderEOFException
    for (int i = 0; i < headerLength; i++) {
      Path brokenFile = generateBrokenWALFile(content, i);
      assertThrows(WALHeaderEOFException.class,
        () -> WALFactory.createStreamReader(FS, brokenFile, UTIL.getConfiguration()));
      assertThrows(WALHeaderEOFException.class,
        () -> WALFactory.createTailingReader(FS, brokenFile, UTIL.getConfiguration(), -1));
      FS.delete(brokenFile, false);
    }
    // partial WAL entries, should be able to read some entries and the last one we will get an EOF
    for (int i = 0; i <= endOffsets.size(); i++) {
      int startOffset;
      int endOffset;
      if (i == 0) {
        startOffset = (int) headerLength;
        endOffset = endOffsets.get(i).intValue();
      } else if (i == endOffsets.size()) {
        startOffset = endOffsets.get(i - 1).intValue();
        endOffset = (int) fileLength;
      } else {
        startOffset = endOffsets.get(i - 1).intValue();
        endOffset = endOffsets.get(i).intValue();
      }
      for (int j = startOffset; j < endOffset; j++) {
        Path brokenFile = generateBrokenWALFile(content, j);
        testReadEntry(brokenFile, i);
        FS.delete(brokenFile, false);
      }
    }
    // partial trailer, should be able to read all the entries but get an EOF when trying read
    // again, as we do not know it is a trailer
    for (int i = endOffsets.get(endOffsets.size() - 1).intValue(); i < fileLength; i++) {
      Path brokenFile = generateBrokenWALFile(content, i);
      testReadEntry(brokenFile, endOffsets.size());
      FS.delete(brokenFile, false);
    }
  }
}

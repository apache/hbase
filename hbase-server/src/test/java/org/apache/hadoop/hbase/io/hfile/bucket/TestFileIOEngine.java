/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;

import org.apache.hadoop.hbase.io.hfile.bucket.TestByteBufferIOEngine.BufferGrabbingDeserializer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Basic test for {@link FileIOEngine}
 */
@Category({IOTests.class, SmallTests.class})
@RunWith(Parameterized.class)
public class TestFileIOEngine {
  private static final Object[] ONE_FILE = parametersFor(1, 4096);
  private static final Object[] TWO_FILES = parametersFor(2, 4096);
  private static final Object[] EIGHT_FILES = parametersFor(8, 4096);
  private static final Object[] MANY_FILES = parametersFor(24, 24 * 4096);
  private static final Object[] EVENLY_DIVIDE = parametersFor(4, 4096);
  private static final Object[] EVENLY_DIVIDE_LOW_STEP = parametersFor(4, 4095);
  private static final Object[] EVENLY_DIVIDE_HIGH_STEP = parametersFor(4, 4093);
  private static final Object[] EVENLY_DIVIDE_MINUS_1 = parametersFor(4, 4092);
  @Rule
  public TestName name = new TestName();
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  // Parameterized on
  private final int fileCount;
  private final long totalSize;

  private String[] filePaths;
  private File[] files;
  private String[] shutdownTestPaths;
  private File[] shutdownTestFiles;
  private FileIOEngine fileIOEngine;
  private int fileSize;
  private long expectedTotalSize;

  private Random random;

  public TestFileIOEngine(int fileCount, long totalSize) {
    this.fileCount = fileCount;
    this.totalSize = totalSize;
  }

  @Parameters(name = "{index}: fileCount={0}, totalSize={1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        ONE_FILE,
        TWO_FILES,
        EIGHT_FILES,
        MANY_FILES,
        EVENLY_DIVIDE,
        EVENLY_DIVIDE_LOW_STEP,
        EVENLY_DIVIDE_HIGH_STEP,
        EVENLY_DIVIDE_MINUS_1
    );
  }

  private static Object[] parametersFor(int fileCount, long totalSize) {
    return new Object[]{fileCount, totalSize};
  }

  @Before
  public void setUp() throws Exception {
    files = new File[fileCount];
    filePaths = new String[fileCount];
    shutdownTestFiles = new File[fileCount];
    shutdownTestPaths = new String[fileCount];
    for (int i = 0; i < fileCount; ++i) {
      files[i] = folder.newFile(String.format("%s_FileCount%d_TotalSize%d_Index%d",
          name.getMethodName(), fileCount, totalSize, i));
      filePaths[i] = files[i].getAbsolutePath();
      shutdownTestFiles[i] = folder.newFile(
          String.format("ForShutdown_%s_FileCount%d_TotalSize%d_Index%d",
              name.getMethodName(), fileCount, totalSize, i));
      shutdownTestPaths[i] = shutdownTestFiles[i].getAbsolutePath();
    }
    fileSize = Ints.checkedCast(LongMath.divide(totalSize, fileCount, RoundingMode.CEILING));
    expectedTotalSize = fileSize * fileCount;
    fileIOEngine = new FileIOEngine(filePaths, totalSize);
    random = new Random();
  }

  @After
  public void tearDown() throws Exception {
    fileIOEngine.shutdown();
  }

  @Test
  public void testFileIOEngineConstructor() throws Exception {
    assertEquals(expectedTotalSize, fileIOEngine.getTotalSize());
    for (File file : files) {
      assertEquals(fileSize, file.length());
    }
  }

  @Test
  public void testIsPersistent() {
    assertTrue(fileIOEngine.isPersistent());
  }

  @Test
  public void testIsSegmented() throws Exception {
    if (fileCount > 1) {
      assertTrue(fileIOEngine.isSegmented());
    } else {
      assertFalse(fileIOEngine.isSegmented());
    }
  }

  public void testAllocationCrossedSegments() throws Exception {
    for (int i = 0; i < fileCount; ++i) {
      long offset = i * fileSize;
      long endOffset = (i + 1) * fileSize;
      assertFalse(fileIOEngine.allocationCrossedSegments(offset, fileSize));
      assertFalse(fileIOEngine.allocationCrossedSegments(endOffset - 1, 1));
      assertTrue(fileIOEngine.allocationCrossedSegments(offset, fileSize + 1));
      assertTrue(fileIOEngine.allocationCrossedSegments(endOffset - 1, 2));
    }
  }

  @Test
  public void testRead() throws Exception {
    assertReadWithOffsetAndLength(0, fileSize);
    assertReadWithOffsetAndLength(0, fileSize / 2);
    assertReadWithOffsetAndLength(fileSize - 1, 1);
    assertReadWithOffsetAndLength(fileSize / 2, fileSize - (fileSize / 2));
  }

  @Test
  public void testWrite() throws Exception {
    assertWriteWithOffsetAndLength(0, fileSize);
    assertWriteWithOffsetAndLength(0, fileSize / 2);
    assertWriteWithOffsetAndLength(fileSize - 1, 1);
    assertWriteWithOffsetAndLength(fileSize / 2, fileSize - (fileSize / 2));
  }

  @Test
  public void testReadCrossSegment() throws Exception {
    assertExceptionOnReadWhenCrossingSegments(0, fileSize + 1);
    assertExceptionOnReadWhenCrossingSegments(fileSize - 1, 2);
    assertExceptionOnReadWhenCrossingSegments(fileSize / 2, fileSize - (fileSize / 2) + 1);
  }

  @Test
  public void testWriteCrossSegment() throws Exception {
    assertExceptionOnWriteWhenCrossingSegments(0, fileSize + 1);
    assertExceptionOnWriteWhenCrossingSegments(fileSize - 1, 2);
    assertExceptionOnWriteWhenCrossingSegments(fileSize / 2, fileSize - (fileSize / 2) + 1);
  }

  @Test
  public void testReadNegativeOffset() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
    fileIOEngine.read(-1, 1, deserializer);
  }

  @Test
  public void testWriteNegativeOffset() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    fileIOEngine.write(ByteBuffer.allocate(1), -1);
  }

  @Test
  public void testReadOutOfBoundsOffset() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
    fileIOEngine.read(1, (int) expectedTotalSize, deserializer);
  }

  @Test
  public void testWriteOutOfBoundsOffset() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    fileIOEngine.write(ByteBuffer.allocate(1), expectedTotalSize);
  }

  @Test
  public void testSync() throws Exception {
    FileIOEngine spyFileIOEngine = spy(fileIOEngine);
    List<FileIOEngine.SegmentFile> spySegments =
        new ArrayList<>(spyFileIOEngine.getSegments().size());
    for (FileIOEngine.SegmentFile segmentFile : spyFileIOEngine.getSegments()) {
      spySegments.add(spy(segmentFile));
    }
    when(fileIOEngine.getSegments()).thenReturn(spySegments);

    spyFileIOEngine.sync();
    for (FileIOEngine.SegmentFile spySegment : spySegments) {
      verify(spySegment, times(1)).fsync();
    }
  }

  @Test
  public void testShutdown() throws Exception {
    FileIOEngine shutdownFileIOEngine = new FileIOEngine(shutdownTestPaths, fileSize);
    FileIOEngine spyShutdownFileIOEngine = spy(shutdownFileIOEngine);

    List<FileIOEngine.SegmentFile> spySegments =
        new ArrayList<>(spyShutdownFileIOEngine.getSegments().size());
    for (FileIOEngine.SegmentFile segment : spyShutdownFileIOEngine.getSegments()) {
      spySegments.add(spy(segment));
    }
    when(spyShutdownFileIOEngine.getSegments()).thenReturn(spySegments);

    spyShutdownFileIOEngine.shutdown();

    for (FileIOEngine.SegmentFile spySegment : spySegments) {
      verify(spySegment, times(1)).close();
      verify(spySegment, times(1)).closeChannel();
      verify(spySegment, times(1)).closeRandomAccessFile();
    }
  }

  private void assertReadWithOffsetAndLength(long segmentOffset, int length) throws Exception {
    for (int i = 0; i < fileCount; ++i) {
      long offset = i * fileSize + segmentOffset;
      byte[] expected = new byte[length];
      random.nextBytes(expected);
      try (RandomAccessFile raf = new RandomAccessFile(files[i], "rw")) {
        raf.seek(segmentOffset);
        raf.write(expected);
        BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
        fileIOEngine.read(offset, length, deserializer);
        ByteBuff byteBuff = deserializer.getDeserializedByteBuff();
        assertArrayEquals(String.format("off=%d, len=%d", offset, length), expected,
            byteBuff.array());
      }
    }
  }

  private void assertWriteWithOffsetAndLength(long segmentOffset, int length) throws Exception {
    for (int i = 0; i < fileCount; ++i) {
      long offset = i * fileSize + segmentOffset;
      byte[] expected = new byte[length];
      random.nextBytes(expected);
      fileIOEngine.write(ByteBuffer.wrap(expected), offset);
      try (RandomAccessFile raf = new RandomAccessFile(files[i], "r")) {
        byte[] actual = new byte[length];
        raf.seek(segmentOffset);
        raf.read(actual);
        assertArrayEquals(String.format("off=%d, len=%d", offset, length), expected, actual);
      }
    }
  }

  private void assertExceptionOnReadWhenCrossingSegments(long segmentOffset, int length)
      throws Exception {
    for (int i = 0; i < fileCount; ++i) {
      try {
        long offset = (i * fileSize) + segmentOffset;
        BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
        fileIOEngine.read(offset, length, deserializer);
      } catch (IllegalArgumentException e) {
        continue;
      }
      fail(String.format("off=%d, len=%d", segmentOffset, length));
    }
  }

  private void assertExceptionOnWriteWhenCrossingSegments(long segmentOffset, int length)
      throws Exception {
    for (int i = 0; i < fileCount; ++i) {
      try {
        long offset = (i * fileSize) + segmentOffset;
        fileIOEngine.write(ByteBuffer.allocate(length), offset);
      } catch (IllegalArgumentException e) {
        continue;
      }
      fail(String.format("off=%d, len=%d", segmentOffset, length));
    }
  }

}

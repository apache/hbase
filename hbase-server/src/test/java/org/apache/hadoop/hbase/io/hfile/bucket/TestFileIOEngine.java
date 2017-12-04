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

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for {@link FileIOEngine}
 */
@Category(SmallTests.class)
public class TestFileIOEngine {

  private static final long TOTAL_CAPACITY = 6 * 1024 * 1024; // 6 MB
  private static final String[] FILE_PATHS = {"testFileIOEngine1", "testFileIOEngine2",
      "testFileIOEngine3"};
  private static final long SIZE_PER_FILE = TOTAL_CAPACITY / FILE_PATHS.length; // 2 MB per File
  private final static List<Long> boundaryStartPositions = new ArrayList<Long>();
  private final static List<Long> boundaryStopPositions = new ArrayList<Long>();

  private FileIOEngine fileIOEngine;

  static {
    boundaryStartPositions.add(0L);
    for (int i = 1; i < FILE_PATHS.length; i++) {
      boundaryStartPositions.add(SIZE_PER_FILE * i - 1);
      boundaryStartPositions.add(SIZE_PER_FILE * i);
      boundaryStartPositions.add(SIZE_PER_FILE * i + 1);
    }
    for (int i = 1; i < FILE_PATHS.length; i++) {
      boundaryStopPositions.add(SIZE_PER_FILE * i - 1);
      boundaryStopPositions.add(SIZE_PER_FILE * i);
      boundaryStopPositions.add(SIZE_PER_FILE * i + 1);
    }
    boundaryStopPositions.add(SIZE_PER_FILE * FILE_PATHS.length - 1);
  }

  @Before
  public void setUp() throws IOException {
    fileIOEngine = new FileIOEngine(TOTAL_CAPACITY, FILE_PATHS);
  }

  @After
  public void cleanUp() throws IOException {
    fileIOEngine.shutdown();
    for (String filePath : FILE_PATHS) {
      File file = new File(filePath);
      if (file.exists()) {
        file.delete();
      }
    }
  }

  @Test
  public void testFileIOEngine() throws IOException {
    for (int i = 0; i < 500; i++) {
      int len = (int) Math.floor(Math.random() * 100) + 1;
      long offset = (long) Math.floor(Math.random() * TOTAL_CAPACITY % (TOTAL_CAPACITY - len));
      if (i < boundaryStartPositions.size()) {
        // make the boundary start positon
        offset = boundaryStartPositions.get(i);
      } else if ((i - boundaryStartPositions.size()) < boundaryStopPositions.size()) {
        // make the boundary stop positon
        offset = boundaryStopPositions.get(i - boundaryStartPositions.size()) - len + 1;
      } else if (i % 2 == 0) {
        // make the cross-files block writing/reading
        offset = Math.max(1, i % FILE_PATHS.length) * SIZE_PER_FILE - len / 2;
      }
      byte[] data1 = new byte[len];
      for (int j = 0; j < data1.length; ++j) {
        data1[j] = (byte) (Math.random() * 255);
      }
      byte[] data2 = new byte[len];
      fileIOEngine.write(ByteBuffer.wrap(data1), offset);
      fileIOEngine.read(ByteBuffer.wrap(data2), offset);
      assertArrayEquals(data1, data2);
    }
  }

  @Test
  public void testFileIOEngineHandlesZeroLengthInput() throws IOException {
    byte[] data1 = new byte[0];


    byte[] data2 = new byte[0];
    fileIOEngine.write(ByteBuffer.wrap(data1), 0);
    fileIOEngine.read(ByteBuffer.wrap(data2), 0);
    assertArrayEquals(data1, data2);
  }

  @Test
  public void testClosedChannelException() throws IOException {
    fileIOEngine.closeFileChannels();
    int len = 5;
    long offset = 0L;
    byte[] data1 = new byte[len];
    for (int j = 0; j < data1.length; ++j) {
      data1[j] = (byte) (Math.random() * 255);
    }
    byte[] data2 = new byte[len];
    fileIOEngine.write(ByteBuffer.wrap(data1), offset);
    fileIOEngine.read(ByteBuffer.wrap(data2), offset);
    assertArrayEquals(data1, data2);
  }
}

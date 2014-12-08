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

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for {@link ByteBufferIOEngine}
 */
@Category(SmallTests.class)
public class TestByteBufferIOEngine {

  @Test
  public void testByteBufferIOEngine() throws Exception {
    int capacity = 32 * 1024 * 1024; // 32 MB
    int testNum = 100;
    int maxBlockSize = 64 * 1024;
    ByteBufferIOEngine ioEngine = new ByteBufferIOEngine(capacity, false);
    int testOffsetAtStartNum = testNum / 10;
    int testOffsetAtEndNum = testNum / 10;
    for (int i = 0; i < testNum; i++) {
      byte val = (byte) (Math.random() * 255);
      int blockSize = (int) (Math.random() * maxBlockSize);
      if (blockSize == 0) {
        blockSize = 1;
      }
      byte[] byteArray = new byte[blockSize];
      for (int j = 0; j < byteArray.length; ++j) {
        byteArray[j] = val;
      }
      ByteBuffer srcBuffer = ByteBuffer.wrap(byteArray);
      int offset = 0;
      if (testOffsetAtStartNum > 0) {
        testOffsetAtStartNum--;
        offset = 0;
      } else if (testOffsetAtEndNum > 0) {
        testOffsetAtEndNum--;
        offset = capacity - blockSize;
      } else {
        offset = (int) (Math.random() * (capacity - maxBlockSize));
      }
      ioEngine.write(srcBuffer, offset);
      ByteBuffer dstBuffer = ByteBuffer.allocate(blockSize);
      ioEngine.read(dstBuffer, offset);
      byte[] byteArray2 = dstBuffer.array();
      for (int j = 0; j < byteArray.length; ++j) {
        assertTrue(byteArray[j] == byteArray2[j]);
      }
    }
    assert testOffsetAtStartNum == 0;
    assert testOffsetAtEndNum == 0;
  }
}

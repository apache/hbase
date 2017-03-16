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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.hfile.bucket.TestByteBufferIOEngine.BufferGrabbingDeserializer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for {@link FileIOEngine}
 */
@Category({IOTests.class, SmallTests.class})
public class TestFileIOEngine {
  @Test
  public void testFileIOEngine() throws IOException {
    long totalCapacity = 6 * 1024 * 1024; // 6 MB
    String[] filePaths = { "testFileIOEngine1", "testFileIOEngine2",
        "testFileIOEngine3" };
    long sizePerFile = totalCapacity / filePaths.length; // 2 MB per File
    List<Long> boundaryStartPositions = new ArrayList<Long>();
    boundaryStartPositions.add(0L);
    for (int i = 1; i < filePaths.length; i++) {
      boundaryStartPositions.add(sizePerFile * i - 1);
      boundaryStartPositions.add(sizePerFile * i);
      boundaryStartPositions.add(sizePerFile * i + 1);
    }
    List<Long> boundaryStopPositions = new ArrayList<Long>();
    for (int i = 1; i < filePaths.length; i++) {
      boundaryStopPositions.add(sizePerFile * i - 1);
      boundaryStopPositions.add(sizePerFile * i);
      boundaryStopPositions.add(sizePerFile * i + 1);
    }
    boundaryStopPositions.add(sizePerFile * filePaths.length - 1);
    FileIOEngine fileIOEngine = new FileIOEngine(totalCapacity, filePaths);
    try {
      for (int i = 0; i < 500; i++) {
        int len = (int) Math.floor(Math.random() * 100);
        long offset = (long) Math.floor(Math.random() * totalCapacity % (totalCapacity - len));
        if (i < boundaryStartPositions.size()) {
          // make the boundary start positon
          offset = boundaryStartPositions.get(i);
        } else if ((i - boundaryStartPositions.size()) < boundaryStopPositions.size()) {
          // make the boundary stop positon
          offset = boundaryStopPositions.get(i - boundaryStartPositions.size()) - len + 1;
        } else if (i % 2 == 0) {
          // make the cross-files block writing/reading
          offset = Math.max(1, i % filePaths.length) * sizePerFile - len / 2;
        }
        byte[] data1 = new byte[len];
        for (int j = 0; j < data1.length; ++j) {
          data1[j] = (byte) (Math.random() * 255);
        }
        fileIOEngine.write(ByteBuffer.wrap(data1), offset);
        BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
        fileIOEngine.read(offset, len, deserializer);
        ByteBuff data2 = deserializer.getDeserializedByteBuff();
        for (int j = 0; j < data1.length; ++j) {
          assertTrue(data1[j] == data2.get(j));
        }
      }
    } finally {
      fileIOEngine.shutdown();
      for (String filePath : filePaths) {
        File file = new File(filePath);
        if (file.exists()) {
          file.delete();
        }
      }
    }

  }
}

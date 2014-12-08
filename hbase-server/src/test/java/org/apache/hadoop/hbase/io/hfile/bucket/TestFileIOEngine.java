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

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for {@link FileIOEngine}
 */
@Category(SmallTests.class)
public class TestFileIOEngine {
  @Test
  public void testFileIOEngine() throws IOException {
    int size = 2 * 1024 * 1024; // 2 MB
    String filePath = "testFileIOEngine";
    try {
      FileIOEngine fileIOEngine = new FileIOEngine(filePath, size);
      for (int i = 0; i < 50; i++) {
        int len = (int) Math.floor(Math.random() * 100);
        long offset = (long) Math.floor(Math.random() * size % (size - len));
        byte[] data1 = new byte[len];
        for (int j = 0; j < data1.length; ++j) {
          data1[j] = (byte) (Math.random() * 255);
        }
        byte[] data2 = new byte[len];
        fileIOEngine.write(ByteBuffer.wrap(data1), offset);
        fileIOEngine.read(ByteBuffer.wrap(data2), offset);
        for (int j = 0; j < data1.length; ++j) {
          assertTrue(data1[j] == data2[j]);
        }
      }
    } finally {
      File file = new File(filePath);
      if (file.exists()) {
        file.delete();
      }
    }

  }
}

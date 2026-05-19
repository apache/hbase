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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.apache.hadoop.hbase.io.hfile.bucket.TestByteBufferIOEngine.createBucketEntry;
import static org.apache.hadoop.hbase.io.hfile.bucket.TestByteBufferIOEngine.getByteBuff;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Basic test for {@link ExclusiveMemoryMmapIOEngine}
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestExclusiveMemoryMmapEngine {

  @Test
  public void testFileMmapEngine() throws IOException {
    int size = 2 * 1024 * 1024; // 2 MB
    String filePath = "testFileMmapEngine";
    try {
      ExclusiveMemoryMmapIOEngine fileMmapEngine = new ExclusiveMemoryMmapIOEngine(filePath, size);
      for (int i = 0; i < 50; i++) {
        int len = (int) Math.floor(Math.random() * 100);
        long offset = (long) Math.floor(Math.random() * size % (size - len));
        int val = (int) (Math.random() * 255);

        // write
        ByteBuff src = TestByteBufferIOEngine.createByteBuffer(len, val, i % 2 == 0);
        int pos = src.position(), lim = src.limit();
        fileMmapEngine.write(src, offset);
        src.position(pos).limit(lim);

        // read
        BucketEntry be = createBucketEntry(offset, len);
        fileMmapEngine.read(be);
        ByteBuff dst = getByteBuff(be);

        assertEquals(src.remaining(), len);
        assertEquals(dst.remaining(), len);
        assertEquals(0, ByteBuff.compareTo(src, pos, len, dst, dst.position(), dst.remaining()));
      }
    } finally {
      File file = new File(filePath);
      if (file.exists()) {
        file.delete();
      }
    }
  }
}

package org.apache.hadoop.hbase.consensus.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class TestCachedFileChannel {
  final static Log LOG = LogFactory.getLog(TestCachedFileChannel.class);

  @Test
  public void testNormalFile() throws IOException {
    final int size = 100000;
    final int max = 10000;
    final String path = "/tmp/testNormalFile";
    writeToFile(path, size);
    CachedFileChannel cfc = new CachedFileChannel(new RandomAccessFile(path, "r"), max);

    final int trials = 1000;
    Random rand = new Random();
    long now = System.currentTimeMillis();
    LOG.debug("Setting the seed to " + now);
    rand.setSeed(now);
    for(int i = 0; i < trials; i++) {
      int offset = rand.nextInt(size);
      int length;
      if (rand.nextBoolean()) {
        // read something small that fits in memory.
        length = rand.nextInt(Math.min(max, size-offset));
      } else {
        length = rand.nextInt(size - offset);
      }

      verifyData(cfc, offset, length);
    }

    // do some reads reading all the way to the end.
    int more = 100;
    for(int i = 0; i < more; i++) {
      int length = rand.nextInt((int)(1.5 * max));
      int offset = size - length;
      verifyData(cfc, offset, length);
    }

    new File(path).delete();
  }

  private void verifyData(CachedFileChannel cfc, int offset, int length) throws IOException {
    LOG.debug("Verifying data " + length + " bytes, starting from " + offset);
    ByteBuffer bb = ByteBuffer.allocate(length);
    cfc.read(bb, offset);
    bb.flip();

    for(int i = 0; i < length; ++i) {
      Assert.assertEquals("Mismatch at location " + (offset + i),
          (byte)(offset + i), bb.get());
    }
  }

  private void writeToFile(String path, int size) throws IOException {
    FileOutputStream fsOut = new FileOutputStream(path);
    for(int i = 0; i < size; ++i) {
      fsOut.write(i);
    }
    fsOut.close();
  }
}

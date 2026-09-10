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
package org.apache.hadoop.hbase.ipc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.base.Charsets;
import org.apache.hbase.thirdparty.com.google.common.io.Files;

@Tag(RPCTests.TAG)
@Tag(SmallTests.TAG)
public class TestBufferChain {

  private File tmpFile;

  private static final byte[][] HELLO_WORLD_CHUNKS =
    new byte[][] { "hello".getBytes(Charsets.UTF_8), " ".getBytes(Charsets.UTF_8),
      "world".getBytes(Charsets.UTF_8) };

  @BeforeEach
  public void setup() throws IOException {
    tmpFile = File.createTempFile("TestBufferChain", "txt");
  }

  @AfterEach
  public void teardown() {
    tmpFile.delete();
  }

  @Test
  public void testGetBackBytesWePutIn() {
    ByteBuffer[] bufs = wrapArrays(HELLO_WORLD_CHUNKS);
    BufferChain chain = new BufferChain(bufs);
    assertTrue(Bytes.equals(Bytes.toBytes("hello world"), chain.getBytes()));
  }

  @Test
  public void testLimitOffset() throws IOException {
    ByteBuffer[] bufs = new ByteBuffer[] { stringBuf("XXXhelloYYY", 3, 5), stringBuf(" ", 0, 1),
      stringBuf("XXXXworldY", 4, 5) };
    BufferChain chain = new BufferChain(bufs);
    writeAndVerify(chain, "hello world");
    assertNoRemaining(bufs);
  }

  private ByteBuffer stringBuf(String string, int position, int length) {
    ByteBuffer buf = ByteBuffer.wrap(string.getBytes(Charsets.UTF_8));
    buf.position(position);
    buf.limit(position + length);
    assertTrue(buf.hasRemaining());
    return buf;
  }

  private void assertNoRemaining(ByteBuffer[] bufs) {
    for (ByteBuffer buf : bufs) {
      assertFalse(buf.hasRemaining());
    }
  }

  private ByteBuffer[] wrapArrays(byte[][] arrays) {
    ByteBuffer[] ret = new ByteBuffer[arrays.length];
    for (int i = 0; i < arrays.length; i++) {
      ret[i] = ByteBuffer.wrap(arrays[i]);
    }
    return ret;
  }

  private void writeAndVerify(BufferChain chain, String string) throws IOException {
    FileOutputStream fos = new FileOutputStream(tmpFile);
    FileChannel ch = fos.getChannel();
    try {
      long remaining = string.length();
      while (chain.hasRemaining()) {
        long n = chain.write(ch);
        remaining -= n;
      }
      assertEquals(0, remaining);
    } finally {
      fos.close();
    }
    assertFalse(chain.hasRemaining());
    assertEquals(string, Files.toString(tmpFile, Charsets.UTF_8));
  }
}

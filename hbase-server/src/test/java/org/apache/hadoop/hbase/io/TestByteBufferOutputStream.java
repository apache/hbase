/**
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
package org.apache.hadoop.hbase.io;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestByteBufferOutputStream {
  @Test
  public void testByteBufferReuse() throws IOException {
    Bytes.toBytes("some bytes");
    ByteBuffer bb = ByteBuffer.allocate(16);
    ByteBuffer bbToReuse = write(bb, Bytes.toBytes("some bytes"));
    bbToReuse = write(bbToReuse, Bytes.toBytes("less"));
    assertTrue(bb == bbToReuse);
  }

  private ByteBuffer write(final ByteBuffer bb, final byte [] bytes) throws IOException {
    try (ByteBufferOutputStream bbos = new ByteBufferOutputStream(bb)) {
      bbos.write(bytes);
      assertTrue(Bytes.compareTo(bytes, bbos.toByteArray(0, bytes.length)) == 0);
      bbos.flush();
      return bbos.getByteBuffer();
    }
  }
}
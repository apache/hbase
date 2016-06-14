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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.assertEquals;
@Category({ IOTests.class, SmallTests.class })
public class TestByteBufferPool {

  @Test
  public void testOffheapBBPool() throws Exception {
    boolean directByteBuffer = true;
    testBBPool(10, 100, directByteBuffer);
  }

  @Test
  public void testOnheapBBPool() throws Exception {
    boolean directByteBuffer = false;
    testBBPool(10, 100, directByteBuffer);
  }

  private void testBBPool(int maxPoolSize, int bufferSize, boolean directByteBuffer) {
    ByteBufferPool pool = new ByteBufferPool(bufferSize, maxPoolSize, directByteBuffer);
    for (int i = 0; i < maxPoolSize; i++) {
      ByteBuffer buffer = pool.getBuffer();
      assertEquals(0, buffer.position());
      assertEquals(bufferSize, buffer.limit());
      assertEquals(directByteBuffer, buffer.isDirect());
    }
    assertEquals(0, pool.getQueueSize());
    ByteBuffer bb = directByteBuffer ? ByteBuffer.allocate(bufferSize)
        : ByteBuffer.allocateDirect(bufferSize);
    pool.putbackBuffer(bb);
    assertEquals(0, pool.getQueueSize());
    bb = directByteBuffer ? ByteBuffer.allocateDirect(bufferSize + 1)
        : ByteBuffer.allocate(bufferSize + 1);
    pool.putbackBuffer(bb);
    assertEquals(0, pool.getQueueSize());
  }
}

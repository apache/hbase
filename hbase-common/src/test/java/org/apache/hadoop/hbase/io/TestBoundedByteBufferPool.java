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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestBoundedByteBufferPool {
  final int maxByteBufferSizeToCache = 10;
  final int initialByteBufferSize = 1;
  final int maxToCache = 10;
  BoundedByteBufferPool reservoir;

  @Before
  public void before() {
    this.reservoir =
      new BoundedByteBufferPool(maxByteBufferSizeToCache, initialByteBufferSize, maxToCache);
  }

  @After
  public void after() {
    this.reservoir = null;
  }

  @Test
  public void testEquivalence() {
    ByteBuffer bb = ByteBuffer.allocate(1);
    this.reservoir.putBuffer(bb);
    this.reservoir.putBuffer(bb);
    this.reservoir.putBuffer(bb);
    assertEquals(3, this.reservoir.buffers.size());
  }

  @Test
  public void testGetPut() {
    ByteBuffer bb = this.reservoir.getBuffer();
    assertEquals(initialByteBufferSize, bb.capacity());
    assertEquals(0, this.reservoir.buffers.size());
    this.reservoir.putBuffer(bb);
    assertEquals(1, this.reservoir.buffers.size());
    // Now remove a buffer and don't put it back so reservoir is empty.
    this.reservoir.getBuffer();
    assertEquals(0, this.reservoir.buffers.size());
    // Try adding in a buffer with a bigger-than-initial size and see if our runningAverage works.
    // Need to add then remove, then get a new bytebuffer so reservoir internally is doing
    // allocation
    final int newCapacity = 2;
    this.reservoir.putBuffer(ByteBuffer.allocate(newCapacity));
    assertEquals(1, reservoir.buffers.size());
    this.reservoir.getBuffer();
    assertEquals(0, this.reservoir.buffers.size());
    bb = this.reservoir.getBuffer();
    assertEquals(newCapacity, bb.capacity());
    // Assert that adding a too-big buffer won't happen
    assertEquals(0, this.reservoir.buffers.size());
    this.reservoir.putBuffer(ByteBuffer.allocate(maxByteBufferSizeToCache * 2));
    assertEquals(0, this.reservoir.buffers.size());
    // Assert we can't add more than max allowed instances.
    for (int i = 0; i < maxToCache; i++) {
      this.reservoir.putBuffer(ByteBuffer.allocate(initialByteBufferSize));
    }
    assertEquals(maxToCache, this.reservoir.buffers.size());
  }
}

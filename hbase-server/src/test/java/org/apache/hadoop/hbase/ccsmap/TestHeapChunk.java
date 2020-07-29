/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHeapChunk {

  @Test
  public void testOnHeapNormal() {
    int len = 4 * 1024 * 1024; // 4MB
    long chunkID = 1234;
    OnHeapChunk chunk1 = new OnHeapChunk(chunkID, len);
    Assert.assertEquals(1234, chunk1.getChunkID());
    Assert.assertEquals(0, chunk1.getPosition());
    Assert.assertEquals(len, chunk1.getLimit());
    Assert.assertTrue(chunk1.isPooledChunk());
    Assert.assertEquals(len, chunk1.occupancy());
    Assert.assertEquals(HeapMode.ON_HEAP, chunk1.getHeapMode());
    Assert.assertEquals(chunkID, chunk1.hashCode());
    Assert.assertNotNull(chunk1.getByteBuffer());

    int bytes1 = 1023;
    int startPosition1 = chunk1.allocate(bytes1);
    Assert.assertEquals(0, startPosition1);
    // Since alignment happened, it should start from 1024
    Assert.assertEquals(1024, chunk1.getPosition());
    Assert.assertEquals(len - 1024, chunk1.occupancy());

    int bytes2 = 1025;
    int startPosistion2 = chunk1.allocate(bytes2);
    Assert.assertEquals(1024, startPosistion2);
    // Since alignment happened, it should start from 1024 + (1024 + 8)
    Assert.assertEquals(1024 + 1032, chunk1.getPosition());
    Assert.assertEquals(len - 1024 - 1032, chunk1.occupancy());

    ByteBuffer bb = chunk1.getByteBuffer();
    Assert.assertEquals(len, bb.limit());
    Assert.assertEquals(len, bb.capacity());
    Assert.assertEquals(0, bb.position());

    int len2 = 4096;
    ByteBuffer bb2 = chunk1.asSubByteBuffer(100, len2);
    Assert.assertEquals(len2, bb2.limit());
    Assert.assertEquals(len2, bb2.capacity());
    Assert.assertEquals(0, bb2.position());

    OnHeapChunk chunk2 = new OnHeapChunk(1234, len);
    //As long as chunkID is same, Chunk is the same
    Assert.assertEquals(chunk1, chunk2);

    OnHeapChunk chunk3 = new OnHeapChunk(1235, len, false);
    Assert.assertFalse(chunk3.isPooledChunk());
  }

  @Test
  public void testOffHeapNormal() {
    int len = 4 * 1024 * 1024; // 4MB
    long chunkID = 1234;
    OffHeapChunk chunk1 = new OffHeapChunk(chunkID, len);
    Assert.assertEquals(1234, chunk1.getChunkID());
    Assert.assertEquals(0, chunk1.getPosition());
    Assert.assertEquals(len, chunk1.getLimit());
    Assert.assertTrue(chunk1.isPooledChunk());
    Assert.assertEquals(len, chunk1.occupancy());
    Assert.assertEquals(HeapMode.OFF_HEAP, chunk1.getHeapMode());
    Assert.assertEquals(chunkID, chunk1.hashCode());
    Assert.assertNotNull(chunk1.getByteBuffer());

    int bytes1 = 1023;
    int startPosition1 = chunk1.allocate(bytes1);
    Assert.assertEquals(0, startPosition1);
    // Since alignment happened, it should start from 1024
    Assert.assertEquals(1024, chunk1.getPosition());
    Assert.assertEquals(len - 1024, chunk1.occupancy());

    int bytes2 = 1025;
    int startPosistion2 = chunk1.allocate(bytes2);
    Assert.assertEquals(1024, startPosistion2);
    // Since alignment happened, it should start from 1024 + (1024 + 8)
    Assert.assertEquals(1024 + 1032, chunk1.getPosition());
    Assert.assertEquals(len - 1024 - 1032, chunk1.occupancy());

    ByteBuffer bb = chunk1.getByteBuffer();
    Assert.assertEquals(len, bb.limit());
    Assert.assertEquals(len, bb.capacity());
    Assert.assertEquals(0, bb.position());

    int len2 = 4096;
    ByteBuffer bb2 = chunk1.asSubByteBuffer(100, len2);
    Assert.assertEquals(len2, bb2.limit());
    Assert.assertEquals(len2, bb2.capacity());
    Assert.assertEquals(0, bb2.position());

    OffHeapChunk chunk2 = new OffHeapChunk(1234, len);
    //As long as chunkID is same, Chunk is the same
    Assert.assertEquals(chunk1, chunk2);

    OffHeapChunk chunk3 = new OffHeapChunk(1235, len, false);
    Assert.assertFalse(chunk3.isPooledChunk());
  }

  @Test
  public void testConcurrentWriteOnHeap() throws Exception {
    int len = 4 * 1024 * 1024;
    OnHeapChunk chunk = new OnHeapChunk(1234, len);

    int concurrent = 50;
    final ByteBuffer[] bbArray = new ByteBuffer[concurrent];

    for (int i = 0; i < concurrent; i++) {
      bbArray[i] = chunk.asSubByteBuffer(i * 2049, 1023);
    }

    final AtomicBoolean hasError = new AtomicBoolean(false);
    Thread[] ths = new Thread[concurrent];

    for (int i = 0; i < concurrent; i++) {
      final int thid = i;
      ths[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          ByteBuffer bb = ByteBuffer.allocate(13);
          bb.put((byte) thid);
          bb.putInt(thid);
          bb.putLong(thid);
          bb.flip();
          try {
            Assert.assertEquals(0, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].put((byte) thid);
            Assert.assertEquals(1, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].putInt(thid);
            Assert.assertEquals(1 + 4, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].putLong(thid);
            Assert.assertEquals(1 + 4 + 8, bbArray[thid].position());
            Thread.sleep(100);
            bbArray[thid].put(bb);
            Assert.assertEquals(1 + 4 + 8 + 13, bbArray[thid].position());
          } catch (Throwable e) {
            e.printStackTrace();
            hasError.set(true);
          }
        }
      });
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].start();
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].join();
    }

    Assert.assertFalse(hasError.get());

    for (int j = 0; j < concurrent; j++) {
      bbArray[j].rewind();
      Assert.assertEquals(0, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1 + 4 + 8 + 1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4 + 8, bbArray[j].position());
    }

    ByteBuffer bb = chunk.getByteBuffer();
    bb.rewind();
    for (int j = 0; j < concurrent; j++) {
      bb.position(j * 2049);
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
    }
  }

  @Test
  public void testConcurrentWriteOffHeap() throws Exception {
    int len = 4 * 1024 * 1024; // 4MB
    OffHeapChunk chunk = new OffHeapChunk(1234, len);

    int concurrent = 50;
    final ByteBuffer[] bbArray = new ByteBuffer[concurrent];

    for (int i = 0; i < concurrent; i++) {
      bbArray[i] = chunk.asSubByteBuffer(i * 2049, 1023);
    }

    final AtomicBoolean hasError = new AtomicBoolean(false);
    Thread[] ths = new Thread[concurrent];

    for (int i = 0; i < concurrent; i++) {
      final int thid = i;
      ths[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          ByteBuffer bb = ByteBuffer.allocate(13);
          bb.put((byte) thid);
          bb.putInt(thid);
          bb.putLong(thid);
          bb.flip();
          try {
            Assert.assertEquals(0, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].put((byte) thid);
            Assert.assertEquals(1, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].putInt(thid);
            Assert.assertEquals(1 + 4, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].putLong(thid);
            Assert.assertEquals(1 + 4 + 8, bbArray[thid].position());
            Thread.sleep(1000);
            bbArray[thid].put(bb);
            Assert.assertEquals(1 + 4 + 8 + 13, bbArray[thid].position());
          } catch (Throwable e) {
            e.printStackTrace();
            hasError.set(true);
          }
        }
      });
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].start();
    }

    for (int j = 0; j < concurrent; j++) {
      ths[j].join();
    }

    Assert.assertFalse(hasError.get());

    for (int j = 0; j < concurrent; j++) {
      bbArray[j].rewind();
      Assert.assertEquals(0, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].get());
      Assert.assertEquals(1 + 4 + 8 + 1, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getInt());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4, bbArray[j].position());
      Assert.assertEquals(j, bbArray[j].getLong());
      Assert.assertEquals(1 + 4 + 8 + 1 + 4 + 8, bbArray[j].position());
    }

    ByteBuffer bb = chunk.getByteBuffer();
    bb.rewind();
    for (int j = 0; j < concurrent; j++) {
      bb.position(j * 2049);
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
      Assert.assertEquals(j, bb.get());
      Assert.assertEquals(j, bb.getInt());
      Assert.assertEquals(j, bb.getLong());
    }
  }

}

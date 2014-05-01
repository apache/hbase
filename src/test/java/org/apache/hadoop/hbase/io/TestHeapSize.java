/*
 * Copyright 2009 The Apache Software Foundation
 *
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

import junit.framework.TestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Testing the sizing that HeapSize offers and compares to the size given by
 * ClassSize.
 */
public class TestHeapSize extends TestCase {
  /**
   * Testing the classes that implements HeapSize and are a part of 0.20.
   * Some are not tested here for example BlockIndex which is tested in
   * TestHFile since it is a non public class
   * @throws IOException
   */
  public void testSizes() throws IOException {
    Class<?> cl = null;
    long expected = 0L;
    long actual = 0L;

    //KeyValue
    cl = KeyValue.class;
    expected = ClassSize.estimateBase(cl, false);
    KeyValue kv = new KeyValue();
    actual = kv.heapSize();
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    //Put
    cl = Put.class;
    expected = ClassSize.estimateBase(cl, false);
    //The actual TreeMap is not included in the above calculation
    expected += ClassSize.TREEMAP;
    Put put = new Put(Bytes.toBytes(""));
    actual = put.heapSize();
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CachedBlock Fixed Overhead
    // We really need "deep" sizing but ClassSize does not do this.
    // Perhaps we should do all these more in this style....
    cl = CachedBlock.class;
    actual = CachedBlock.PER_BLOCK_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    expected += ClassSize.estimateBase(String.class, false);
    expected += ClassSize.estimateBase(ByteBuffer.class, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      ClassSize.estimateBase(String.class, true);
      ClassSize.estimateBase(ByteBuffer.class, true);
      assertEquals(expected, actual);
    }

    // Block cache key overhead. Only tests fixed overhead as estimating heap
    // size of strings is hard.
    cl = BlockCacheKey.class;
    actual = BlockCacheKey.FIXED_OVERHEAD;
    expected  = ClassSize.estimateBase(cl, false);
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Currently NOT testing Deep Overheads of many of these classes.
    // Deep overheads cover a vast majority of stuff, but will not be 100%
    // accurate because it's unclear when we're referencing stuff that's already
    // accounted for.  But we have satisfied our two core requirements.
    // Sizing is quite accurate now, and our tests will throw errors if
    // any of these classes are modified without updating overhead sizes.
  }

}

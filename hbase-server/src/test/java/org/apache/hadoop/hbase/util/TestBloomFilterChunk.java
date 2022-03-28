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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestBloomFilterChunk {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBloomFilterChunk.class);

  @Test
  public void testBasicBloom() throws Exception {
    BloomFilterChunk bf1 = new BloomFilterChunk(1000, (float)0.01, Hash.MURMUR_HASH, 0);
    BloomFilterChunk bf2 = new BloomFilterChunk(1000, (float)0.01, Hash.MURMUR_HASH, 0);
    bf1.allocBloom();
    bf2.allocBloom();

    // test 1: verify no fundamental false negatives or positives
    byte[] key1 = {1,2,3,4,5,6,7,8,9};
    byte[] key2 = {1,2,3,4,5,6,7,8,7};

    bf1.add(key1, 0, key1.length);
    bf2.add(key2, 0, key2.length);

    assertTrue(BloomFilterUtil.contains(key1, 0, key1.length, new MultiByteBuff(bf1.bloom), 0,
        (int) bf1.byteSize, bf1.hash, bf1.hashCount));
    assertFalse(BloomFilterUtil.contains(key2, 0, key2.length, new MultiByteBuff(bf1.bloom), 0,
        (int) bf1.byteSize, bf1.hash, bf1.hashCount));
    assertFalse(BloomFilterUtil.contains(key1, 0, key1.length, new MultiByteBuff(bf2.bloom), 0,
        (int) bf2.byteSize, bf2.hash, bf2.hashCount));
    assertTrue(BloomFilterUtil.contains(key2, 0, key2.length, new MultiByteBuff(bf2.bloom), 0,
        (int) bf2.byteSize, bf2.hash, bf2.hashCount));

    byte [] bkey = {1,2,3,4};
    byte [] bval = Bytes.toBytes("this is a much larger byte array");

    bf1.add(bkey, 0, bkey.length);
    bf1.add(bval, 1, bval.length-1);

    assertTrue(BloomFilterUtil.contains(bkey, 0, bkey.length, new MultiByteBuff(bf1.bloom), 0,
        (int) bf1.byteSize, bf1.hash, bf1.hashCount));
    assertTrue(BloomFilterUtil.contains(bval, 1, bval.length - 1, new MultiByteBuff(bf1.bloom),
        0, (int) bf1.byteSize, bf1.hash, bf1.hashCount));
    assertFalse(BloomFilterUtil.contains(bval, 0, bval.length, new MultiByteBuff(bf1.bloom), 0,
        (int) bf1.byteSize, bf1.hash, bf1.hashCount));

    // test 2: serialization & deserialization.
    // (convert bloom to byte array & read byte array back in as input)
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    bf1.writeBloom(new DataOutputStream(bOut));
    ByteBuffer bb = ByteBuffer.wrap(bOut.toByteArray());
    BloomFilterChunk newBf1 = new BloomFilterChunk(1000, (float)0.01,
        Hash.MURMUR_HASH, 0);
    assertTrue(BloomFilterUtil.contains(key1, 0, key1.length, new MultiByteBuff(bb), 0,
        (int) newBf1.byteSize, newBf1.hash, newBf1.hashCount));
    assertFalse(BloomFilterUtil.contains(key2, 0, key2.length, new MultiByteBuff(bb), 0,
        (int) newBf1.byteSize, newBf1.hash, newBf1.hashCount));
    assertTrue(BloomFilterUtil.contains(bkey, 0, bkey.length, new MultiByteBuff(bb), 0,
        (int) newBf1.byteSize, newBf1.hash, newBf1.hashCount));
    assertTrue(BloomFilterUtil.contains(bval, 1, bval.length - 1, new MultiByteBuff(bb), 0,
        (int) newBf1.byteSize, newBf1.hash, newBf1.hashCount));
    assertFalse(BloomFilterUtil.contains(bval, 0, bval.length, new MultiByteBuff(bb), 0,
        (int) newBf1.byteSize, newBf1.hash, newBf1.hashCount));
    assertFalse(BloomFilterUtil.contains(bval, 0, bval.length, new MultiByteBuff(bb), 0,
        (int) newBf1.byteSize, newBf1.hash, newBf1.hashCount));

    System.out.println("Serialized as " + bOut.size() + " bytes");
    assertTrue(bOut.size() - bf1.byteSize < 10); //... allow small padding
  }

  @Test
  public void testBloomFold() throws Exception {
    // test: foldFactor < log(max/actual)
    BloomFilterChunk b = new BloomFilterChunk(1003, (float) 0.01,
        Hash.MURMUR_HASH, 2);
    b.allocBloom();
    long origSize = b.getByteSize();
    assertEquals(1204, origSize);
    for (int i = 0; i < 12; ++i) {
      byte[] ib = Bytes.toBytes(i);
      b.add(ib, 0, ib.length);
    }
    b.compactBloom();
    assertEquals(origSize>>2, b.getByteSize());
    int falsePositives = 0;
    for (int i = 0; i < 25; ++i) {
      byte[] bytes = Bytes.toBytes(i);
      if (BloomFilterUtil.contains(bytes, 0, bytes.length, new MultiByteBuff(b.bloom), 0,
          (int) b.byteSize, b.hash, b.hashCount)) {
        if (i >= 12)
          falsePositives++;
      } else {
        assertFalse(i < 12);
      }
    }
    assertTrue(falsePositives <= 1);

    // test: foldFactor > log(max/actual)
  }

  @Test
  public void testBloomPerf() throws Exception {
    // add
    float err = (float)0.01;
    BloomFilterChunk b = new BloomFilterChunk(10*1000*1000, (float)err, Hash.MURMUR_HASH, 3);
    b.allocBloom();
    long startTime =  EnvironmentEdgeManager.currentTime();
    long origSize = b.getByteSize();
    for (int i = 0; i < 1*1000*1000; ++i) {
      byte[] ib = Bytes.toBytes(i);
      b.add(ib, 0, ib.length);
    }
    long endTime = EnvironmentEdgeManager.currentTime();
    System.out.println("Total Add time = " + (endTime - startTime) + "ms");

    // fold
    startTime = EnvironmentEdgeManager.currentTime();
    b.compactBloom();
    endTime = EnvironmentEdgeManager.currentTime();
    System.out.println("Total Fold time = " + (endTime - startTime) + "ms");
    assertTrue(origSize >= b.getByteSize()<<3);

    // test
    startTime = EnvironmentEdgeManager.currentTime();
    int falsePositives = 0;
    for (int i = 0; i < 2*1000*1000; ++i) {

      byte[] bytes = Bytes.toBytes(i);
      if (BloomFilterUtil.contains(bytes, 0, bytes.length, new MultiByteBuff(b.bloom), 0,
          (int) b.byteSize, b.hash, b.hashCount)) {
        if (i >= 1 * 1000 * 1000)
          falsePositives++;
      } else {
        assertFalse(i < 1*1000*1000);
      }
    }
    endTime = EnvironmentEdgeManager.currentTime();
    System.out.println("Total Contains time = " + (endTime - startTime) + "ms");
    System.out.println("False Positive = " + falsePositives);
    assertTrue(falsePositives <= (1*1000*1000)*err);

    // test: foldFactor > log(max/actual)
  }

  @Test
  public void testSizing() {
    int bitSize = 8 * 128 * 1024; // 128 KB
    double errorRate = 0.025; // target false positive rate

    // How many keys can we store in a Bloom filter of this size maintaining
    // the given false positive rate, not taking into account that the n
    long maxKeys = BloomFilterUtil.idealMaxKeys(bitSize, errorRate);
    assertEquals(136570, maxKeys);

    // A reverse operation: how many bits would we need to store this many keys
    // and keep the same low false positive rate?
    long bitSize2 = BloomFilterUtil.computeBitSize(maxKeys, errorRate);

    // The bit size comes out a little different due to rounding.
    assertTrue(Math.abs(bitSize2 - bitSize) * 1.0 / bitSize < 1e-5);
  }

  @Test
  public void testFoldableByteSize() {
    assertEquals(128, BloomFilterUtil.computeFoldableByteSize(1000, 5));
    assertEquals(640, BloomFilterUtil.computeFoldableByteSize(5001, 4));
  }
}


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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Category({ MiscTests.class, SmallTests.class })
public class TestBloomFilterUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBloomFilterUtil.class);
  private HashKey<byte[]> hashKey;

  @BeforeEach
  public void setup() {
    byte[] SAMPLE_DATA = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    hashKey = new ByteArrayHashKey(SAMPLE_DATA, 0, SAMPLE_DATA.length);
  }

  @Test
  public void testGetHashPairHash64() {
    Hash xxh3 = new XXH3();
    assertInstanceOf(Hash64.class, xxh3);

    long hash64 = ((Hash64) xxh3).hash64(hashKey);
    Pair<Integer, Integer> hashPair = BloomFilterUtil.getHashPair(xxh3, hashKey);
    assertEquals((int) hash64, hashPair.getFirst());
    assertEquals((int) (hash64 >>> 32), hashPair.getSecond());
  }

  @Test
  public void testGetHashPairDoubleHashing() {
    Hash murmurHash = new MurmurHash();
    assertFalse(murmurHash instanceof Hash64);

    int expectedHash1 = murmurHash.hash(hashKey, 0);
    int expectedHash2 = murmurHash.hash(hashKey, expectedHash1);

    Pair<Integer, Integer> hashPair = BloomFilterUtil.getHashPair(murmurHash, hashKey);
    assertEquals(expectedHash1, hashPair.getFirst());
    assertEquals(expectedHash2, hashPair.getSecond());
  }
}

/**
 * Copyright 2014 The Apache Software Foundation
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

package org.apache.hadoop.hbase.util;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup.  See https://code.google.com/p/smhasher/wiki/MurmurHash3 for more
 * details.
 *
 * This implementation in Java was copied from the Mahout repository:
 * https://svn.apache.org/repos/asf/mahout/tags/mahout-0.7/math/src/main/java/org/apache/mahout/math/MurmurHash3.java
 *
 * In simple benchmarks, we found the 32-bit MurmurHash3 to be about 25% in
 * hashing than the 32-bit MurmurHash2. Which is in sync with what the authors
 * claim in
 * https://code.google.com/p/smhasher/source/browse/wiki/MurmurHash3.wiki
 *
 * TODO
 * @gauravm: Test the collision resistance properties of MurmurHash3
 */
public class MurmurHash3 extends Hash {
  private static MurmurHash3 _instance = new MurmurHash3();

  public static Hash getInstance() {
    return _instance;
  }

  @Override
  public int hash(byte[] data, int offset, int len, int seed) {
    int c1 = 0xcc9e2d51;
    int c2 = 0x1b873593;

    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

    for (int i = offset; i < roundedEnd; i += 4) {
      // little endian load order
      int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) |
        ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
      k1 *= c2;

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    // tail
    int k1 = 0;

    switch(len & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
        // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        // fallthrough
      case 1:
        k1 |= data[roundedEnd] & 0xff;
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
        k1 *= c2;
        h1 ^= k1;
      default:
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

}

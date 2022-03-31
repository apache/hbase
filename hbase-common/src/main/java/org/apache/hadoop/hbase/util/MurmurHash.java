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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based lookup. See
 * http://murmurhash.googlepages.com/ for more details.
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by Andrzej Bialecki (ab at
 * getopt org).
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MurmurHash extends Hash {
  private static MurmurHash _instance = new MurmurHash();

  public static Hash getInstance() {
    return _instance;
  }

  @Override
  public <T> int hash(HashKey<T> hashKey, int seed) {
    int m = 0x5bd1e995;
    int r = 24;
    int length = hashKey.length();
    int h = seed ^ length;

    int len_4 = length >> 2;

    for (int i = 0; i < len_4; i++) {
      int i_4 = (i << 2);
      int k = hashKey.get(i_4 + 3);
      k = k << 8;
      k = k | (hashKey.get(i_4 + 2) & 0xff);
      k = k << 8;
      k = k | (hashKey.get(i_4 + 1) & 0xff);
      k = k << 8;
      // noinspection PointlessArithmeticExpression
      k = k | (hashKey.get(i_4 + 0) & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;
    int i_m = len_m;

    if (left != 0) {
      if (left >= 3) {
        h ^= hashKey.get(i_m + 2) << 16;
      }
      if (left >= 2) {
        h ^= hashKey.get(i_m + 1) << 8;
      }
      if (left >= 1) {
        h ^= hashKey.get(i_m);
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }
}

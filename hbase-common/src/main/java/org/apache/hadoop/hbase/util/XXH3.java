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

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * XXH3 64-bit hash implementation based on hash4j library.
 * <p>
 * Provides high throughput, strong dispersion, and minimal memory usage, optimized for modern CPU
 * architectures.
 * @see <a href=
 *      "https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md#xxh3-algorithm-overview">XXH3
 *      Algorithm</a>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XXH3 extends Hash implements Hash64 {
  private static final XXH3 _instance = new XXH3();

  public static Hash getInstance() {
    return _instance;
  }

  @Override
  public <T> long hash64(HashKey<T> hashKey, long seed) {
    Hasher64 hasher = seed == 0L ? Hashing.xxh3_64() : Hashing.xxh3_64(seed);
    return hasher.hashBytesToLong(hashKey, 0, hashKey.length(), HashKeyByteAccess.INSTANCE);
  }

  @Override
  public <T> int hash(HashKey<T> hashKey, int seed) {
    long result64 = hash64(hashKey, seed);
    // Grab only the lower 32 bits.
    return (int) result64;
  }
}

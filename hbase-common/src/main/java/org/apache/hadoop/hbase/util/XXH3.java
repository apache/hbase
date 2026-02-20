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

import java.nio.ByteOrder;
import net.openhft.hashing.Access;
import net.openhft.hashing.LongHashFunction;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * XXH3 64-bit hash implementation based on Zero-Allocation-Hashing.
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
    return LongHashFunction.xx3(seed).hash(hashKey, HashKeyAccess.INSTANCE, 0, hashKey.length());
  }

  @Override
  public <T> int hash(HashKey<T> hashKey, int seed) {
    long result64 = hash64(hashKey, seed);
    // Grab only the lower 32 bits.
    return (int) result64;
  }

  private static class HashKeyAccess extends Access<HashKey<?>> {
    private static final HashKeyAccess INSTANCE = new HashKeyAccess();
    private static final Access<HashKey<?>> REVERSE_INSTANCE = new ReverseAccess<>(INSTANCE);

    @Override
    public long getLong(HashKey<?> hashKey, long offset) {
      return hashKey.getLongLE((int) offset);
    }

    @Override
    public int getInt(HashKey<?> hashKey, long offset) {
      return hashKey.getIntLE((int) offset);
    }

    @Override
    public int getByte(HashKey<?> hashKey, long offset) {
      return hashKey.get((int) offset);
    }

    @Override
    public ByteOrder byteOrder(HashKey<?> hashKey) {
      return ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    protected Access<HashKey<?>> reverseAccess() {
      return REVERSE_INSTANCE;
    }
  }

  private static class ReverseAccess<T> extends Access<T> {
    final Access<T> access;

    private ReverseAccess(Access<T> access) {
      this.access = access;
    }

    public long getLong(T input, long offset) {
      return Long.reverseBytes(this.access.getLong(input, offset));
    }

    public long getUnsignedInt(T input, long offset) {
      return Long.reverseBytes(this.access.getUnsignedInt(input, offset)) >>> 32;
    }

    public int getInt(T input, long offset) {
      return Integer.reverseBytes(this.access.getInt(input, offset));
    }

    public int getUnsignedShort(T input, long offset) {
      return Integer.reverseBytes(this.access.getUnsignedShort(input, offset)) >>> 16;
    }

    public int getShort(T input, long offset) {
      return Integer.reverseBytes(this.access.getShort(input, offset)) >> 16;
    }

    public int getUnsignedByte(T input, long offset) {
      return this.access.getUnsignedByte(input, offset);
    }

    public int getByte(T input, long offset) {
      return this.access.getByte(input, offset);
    }

    public ByteOrder byteOrder(T input) {
      return ByteOrder.LITTLE_ENDIAN == this.access.byteOrder(input)
        ? ByteOrder.BIG_ENDIAN
        : ByteOrder.LITTLE_ENDIAN;
    }

    protected Access<T> reverseAccess() {
      return this.access;
    }
  }
}
